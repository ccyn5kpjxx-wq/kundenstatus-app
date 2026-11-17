"""Reviewed local ALZURA product-GET implementation, packaged with relative imports.
No startup requests, token reads or subscription/scope changes. Native actual-call
ledger hard cap remains9900/month in both quote services; order and paid
distributorList operations are absent from the reader allowlist.
"""
SOURCE_SHA256 = {'alzura_readonly_catalog.py': 'b6d40f601af3a0e12470e71c0d7144c7391d3ebab12f7dd08fac03df77e26e42', 'alzura_live_products.py': '245d06defaa61f2bb569fc58a6a0df9477a6d14398d4ad6175ff3e812c7f1e27', 'shop_live_quotes.py': '56cadd630c38bc36269ffd3c7642f113f969b29d6c3c9f3c87942a856f2d15d3', 'shop_parts_catalog.py': 'd22da5b58f0c5ea5bffdfcb43ff514e2f85d77d363df676442fddd1240fc8bbf', 'shop_parts_provider.py': '83faec1b046f4ce7aede0caf62faee75834263a99f2bb64a40c93003d26fb075'}
