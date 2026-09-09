"""Token codec: mint, verify, tamper, expiry, rotation (REQUIREMENTS sections 11, 12)."""

from __future__ import annotations

import pytest
from agent_wait import (
    FakeClock,
    StaticKeyProvider,
    TokenCodec,
    TokenExpired,
    TokenInvalid,
    Wait,
    WaitPolicy,
)

KEYS = StaticKeyProvider({"k1": b"key-one"}, "k1")


def make_wait(**overrides: object) -> Wait:
    fields: dict[str, object] = {
        "wait_id": "01JWAIT00000000000000000A",
        "thread_id": "order-4471",
        "framework": "stub",
        "interrupt_id": "int-1",
        "checkpoint_id": "ckpt-1",
        "idempotency_key": "idem",
        "question": {"amount": 41000},
        "binding": "abcdef0123456789" + "0" * 48,
        "policy": WaitPolicy(allowed_actions=("approve", "reject")),
    }
    fields.update(overrides)
    return Wait(**fields)  # type: ignore[arg-type]


def test_token_has_the_documented_shape() -> None:
    codec = TokenCodec(KEYS, clock=FakeClock())
    token = codec.mint(make_wait())

    prefix, kid, wait_id, exp, binding16, actions, mac = token.split(".")

    assert prefix == "aw1"
    assert kid == "k1"
    assert wait_id == "01JWAIT00000000000000000A"
    assert binding16 == "abcdef0123456789"
    assert actions == "approve+reject"
    assert len(mac) == 27
    assert int(exp) > 0


def test_verify_returns_the_claims() -> None:
    clock = FakeClock()
    codec = TokenCodec(KEYS, clock=clock)
    claims = codec.verify(codec.mint(make_wait()))

    assert claims.wait_id == "01JWAIT00000000000000000A"
    assert claims.binding16 == "abcdef0123456789"
    assert claims.actions == ("approve", "reject")
    assert claims.exp == int(clock.now()) + 7 * 86400


@pytest.mark.parametrize(
    ("mangle", "why"),
    [
        (lambda t: t[:-1] + "Z", "signature"),
        (lambda t: t.replace("approve+reject", "approve+reject+refund_everything"), "actions"),
        (lambda t: t.replace("aw1.", "aw2."), "version"),
        (lambda t: t.replace(".k1.", ".k9."), "unknown key"),
        (lambda t: "not-a-token", "shape"),
        (lambda t: ".".join(t.split(".")[:-1]), "missing mac"),
    ],
)
def test_tampering_is_rejected(mangle: object, why: str) -> None:
    codec = TokenCodec(KEYS, clock=FakeClock())
    token = codec.mint(make_wait())

    with pytest.raises(TokenInvalid):
        codec.verify(mangle(token))  # type: ignore[operator]


def test_a_bad_expiry_field_is_rejected() -> None:
    codec = TokenCodec(KEYS, clock=FakeClock())
    parts = codec.mint(make_wait()).split(".")
    parts[3] = "tomorrow"

    with pytest.raises(TokenInvalid):
        codec.verify(".".join(parts))


def test_expiry_is_the_tokens_own_and_defaults_to_seven_days() -> None:
    clock = FakeClock()
    codec = TokenCodec(KEYS, clock=clock)
    token = codec.mint(make_wait())

    clock.advance(7 * 86400 - 1)
    assert codec.verify(token).wait_id  # still fine

    clock.advance(2)
    with pytest.raises(TokenExpired):
        codec.verify(token)


def test_rotation_keeps_the_previous_key_verifiable() -> None:
    """Tokens already sitting in inboxes must survive a key rotation."""
    clock = FakeClock()
    old = TokenCodec(StaticKeyProvider({"k1": b"key-one"}, "k1"), clock=clock)
    token = old.mint(make_wait())

    rotated = TokenCodec(StaticKeyProvider({"k2": b"key-two", "k1": b"key-one"}, "k2"), clock=clock)

    assert rotated.verify(token).wait_id == "01JWAIT00000000000000000A"
    assert rotated.mint(make_wait()).split(".")[1] == "k2", "new tokens use the new key"


def test_a_retired_key_stops_verifying() -> None:
    clock = FakeClock()
    old = TokenCodec(StaticKeyProvider({"k1": b"key-one"}, "k1"), clock=clock)
    token = old.mint(make_wait())

    fully_rotated = TokenCodec(StaticKeyProvider({"k3": b"key-three", "k2": b"key-two"}, "k3"), clock=clock)

    with pytest.raises(TokenInvalid):
        fully_rotated.verify(token)


def test_env_key_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_wait import EnvKeyProvider

    monkeypatch.setenv("AGENT_WAIT_SIGNING_KEY", "secret-one")
    monkeypatch.setenv("AGENT_WAIT_SIGNING_KID", "kA")
    monkeypatch.setenv("AGENT_WAIT_PREVIOUS_KEY", "secret-zero")
    monkeypatch.setenv("AGENT_WAIT_PREVIOUS_KID", "k9")
    provider = EnvKeyProvider()

    assert provider.current_kid() == "kA"
    assert set(provider.keys()) == {"kA", "k9"}

    monkeypatch.delenv("AGENT_WAIT_SIGNING_KEY")
    with pytest.raises(TokenInvalid):
        provider.keys()


def test_static_provider_refuses_a_current_kid_it_does_not_hold() -> None:
    with pytest.raises(ValueError, match="not among"):
        StaticKeyProvider({"k1": b"x"}, "k2")


def test_a_non_string_token_is_rejected() -> None:
    codec = TokenCodec(KEYS, clock=FakeClock())
    with pytest.raises(TokenInvalid):
        codec.verify(None)  # type: ignore[arg-type]
