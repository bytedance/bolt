#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) 2025 ByteDance Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import sys
import subprocess
import re

# ================= CONFIGURATION =================

# --- ASF CATEGORY A (Permissive) ---
CAT_A_PATTERNS = [
    r"Apache.*2", r"Apache.*1\.1", r"PHP", r"MX4J",
    r"MIT", r"X11", r"ISC", r"ICU", r"W3C", r"X\.Net",
    r"BSD-[23]-Clause", r"^BSD$", r"0BSD", r"DOM4J", r"PostgreSQL", r"EDL", r"LBNL",
    r"Zlib", r"libpng",
    r"Python", r"PSF", r"PIL",
    r"Ms-PL", r"Microsoft Public",
    r"Boost", r"BSL",
    r"CC0", r"Public Domain", r"Unlicense", r"WTF",
    r"Unicode", r"Zope", r"ACE", r"UPL", r"Open Grid",
    r"Mulan", r"Blue Oak", r"EPICS", r"TCL", r"Adobe Postscript",
    r"DejaVu", r"OFL", r"SIL", r"NCSA",
    r"curl", r"bzip2"
]

# --- ASF CATEGORY B (Weak Copyleft - Binary Only) ---
CAT_B_PATTERNS = [
    r"CDDL",
    r"EPL", r"Eclipse",
    r"IPL", r"CPL",
    r"MPL", r"Mozilla",
    r"SPL",
    r"OSL", r"Open Software License",
    r"Erlang",
    r"Ruby",
    r"UnRAR",
    r"Ubuntu Font", r"IPA Font",
    r"OpenSSL"
]

# --- ASF CATEGORY X (Forbidden) ---
CAT_X_PATTERNS = [
    r"GPL", r"LGPL", r"AGPL", 
    r"BCL", r"Intel Simplified", r"JSR", r"Satori",
    r"Microsoft Limited", r"Ms-LPL",
    r"Amazon", r"ASL",
    r"Redis", r"RSAL",
    r"Booz Allen", r"Confluent", r"Business Source",
    r"Commons Clause",
    r"Non-Commercial", r"CC.*NC", 
    r"BSD-4-Clause", r"BSD-4", r"Facebook BSD", 
    r"NPL", r"QPL", r"Sleepycat", r"SSPL", r"CPOL", 
    r"JSON", r"Solipsistic", r"Don't Be A Dick"
]

IGNORE_PACKAGES = []
IGNORE_BUILD_TOOLS = True

# =================================================

def get_conan_graph():
    try:
        cmd = ["conan", "graph", "info", ".", "--format=json"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except Exception as e:
        print(f"❌ Error running Conan: {e}")
        sys.exit(1)

def categorize_single_license(license_name):
    """
    Returns (status_code, category_name)
    0: Category A (Best)
    1: Category B (Good)
    2: Category X (Bad)
    3: Unknown (Bad)
    """
    if not license_name: return 3, "Missing"

    # Priority: Forbidden check matches first
    for pattern in CAT_X_PATTERNS:
        if re.search(pattern, license_name, re.IGNORECASE):
            return 2, f"Cat X ({pattern})"

    for pattern in CAT_A_PATTERNS:
        if re.search(pattern, license_name, re.IGNORECASE):
            return 0, "Category A"

    for pattern in CAT_B_PATTERNS:
        if re.search(pattern, license_name, re.IGNORECASE):
            return 1, "Category B"

    return 3, "Unknown"

def clean_ref_name(ref):
    """Removes the hash part from conan references (e.g. lib/1.0#abc -> lib/1.0)"""
    if not ref: return ""
    return ref.split('#')[0]

def truncate_str(text, length):
    """Truncates string with ellipses"""
    if len(text) > length:
        return text[:length-3] + "..."
    return text

def main():
    # Setup Table Layout
    # Total Width: 35 + 9 + 25 + 18 + 10 + separators ~ 105 chars
    FMT_ROW = "{:<35} | {:<8} | {:<25} | {:<18} | {}"
    SEP_LINE = "-" * 105

    print("=" * 105)
    print(f"🕵️  LICENSE COMPLIANCE CHECK (ASF POLICY)")
    print("=" * 105)

    data = get_conan_graph()
    graph = data.get("graph", {})
    nodes = graph.get("nodes", {}) if isinstance(graph, dict) else data
    node_iterator = nodes.values() if isinstance(nodes, dict) else nodes

    checked_keys = set()
    failed_packages = []
    warning_packages = []
    stats = {"CatA": 0, "CatB": 0, "Tool": 0, "Fail": 0}

    # Print Header
    print(FMT_ROW.format("Package", "Context", "License", "ASF Category", "Status"))
    print(SEP_LINE)

    for node in node_iterator:
        if str(node.get("id")) == "0": continue
        
        ref = node.get("ref")
        if not ref or any(ign in ref for ign in IGNORE_PACKAGES): continue

        context = node.get("context", "host")
        unique_key = f"{ref}::{context}"
        if unique_key in checked_keys: continue
        checked_keys.add(unique_key)

        # Clean display name (remove hash)
        display_ref = clean_ref_name(ref)
        display_ref = truncate_str(display_ref, 35)

        if IGNORE_BUILD_TOOLS and context == "build":
            print(FMT_ROW.format(display_ref, context, "-", "-", "⏭️  Tool"))
            stats["Tool"] += 1
            continue

        license_field = node.get("license")
        licenses = []
        if isinstance(license_field, str): licenses = [license_field]
        elif isinstance(license_field, list): licenses = license_field
        
        raw_lic_str = ", ".join(licenses) if licenses else "(None)"
        display_lic = truncate_str(raw_lic_str, 25)

        if not licenses:
            print(FMT_ROW.format(display_ref, context, display_lic, "Unknown", "⚠️  WARN"))
            warning_packages.append(clean_ref_name(ref))
            continue

        # Logic Analysis
        best_code = 3 
        final_label = "Unknown"
        failure_reason = "Unknown"

        for lic in licenses:
            code, label = categorize_single_license(lic)
            if code < best_code:
                best_code = code
                final_label = label
            if code == 2: failure_reason = label

        is_pass = False
        if best_code == 0:
            is_pass = True
            stats["CatA"] += 1
        elif best_code == 1:
            is_pass = True
            stats["CatB"] += 1
        else:
            is_pass = False
            stats["Fail"] += 1
            if best_code == 2: final_label = failure_reason
            elif best_code == 3: final_label = "Unknown"

        status_icon = "✅ OK" if is_pass else "❌ FAIL"
        
        # Print Row
        print(FMT_ROW.format(display_ref, context, display_lic, final_label, status_icon))

        if not is_pass:
            failed_packages.append({
                "ref": clean_ref_name(ref), 
                "reason": final_label, 
                "lic": licenses
            })

    # Summary
    print(SEP_LINE)
    print(f"📊 SUMMARY:")
    print(f"   Total Unique:  {len(checked_keys)}")
    print(f"   Category A:    {stats['CatA']} (Source Compatible)")
    print(f"   Category B:    {stats['CatB']} (Binary Only)")
    print(f"   Skipped Tools: {stats['Tool']}")
    
    if warning_packages:
        print(f"   ⚠️  Warnings:     {len(warning_packages)} (No license metadata)")

    if failed_packages:
        print(f"   ❌ Failures:     {len(failed_packages)}")
        print("=" * 105)
        print("❌ CRITICAL: Incompatible Packages Found:")
        for fail in failed_packages:
            print(f"   - {fail['ref']:<35} Reason: {fail['reason']:<20} License: {fail['lic']}")
            if "Unknown" in fail['reason']:
                 print(f"     Hint: Check regex for '{fail['lic']}'")
        sys.exit(1)
    
    print("=" * 105)
    print("✨ SUCCESS: All dependencies comply with ASF Policy.")
    sys.exit(0)

if __name__ == "__main__":
    main()

