"""Fail CI if line coverage in reports/coverage.xml is below the floor."""

import sys
import xml.etree.ElementTree as ET

FLOOR = 85.0

root = ET.parse("reports/coverage.xml").getroot()
rate = float(root.get("line-rate", "0")) * 100
print(f"total line coverage: {rate:.1f}% (floor {FLOOR:.0f}%)")
sys.exit(0 if rate >= FLOOR else 1)
