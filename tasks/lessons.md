# Lessons Learned

Corrections and patterns to avoid repeating mistakes.

---

## 2026-05-01: Diversion Value Framing (CRITICAL)

**Issue:** Initial analysis used confusion matrix framing, treating XRF disagreements with the block model as "errors."

**Why it's wrong:** The block model is NOT ground truth - it's an estimate from blast hole sampling with its own errors. The XRF provides additional real-time information.

**Correct framing:**
- **Diversions (XRF ≠ Block Model) = VALUE CREATION opportunities**
- **Ore Recovery** (XRF=ORE, Block=WASTE): XRF found ore the model missed
- **Dilution Prevention** (XRF=WASTE, Block=ORE): XRF found waste the model thought was ore
- The question isn't "how accurate is XRF?" but "how much value do diversions capture?"

**Rule:** When analyzing sensor vs model disagreement, focus on the VALUE of disagreements, not their frequency as "errors."

---

## 2026-05-01: Serverless Compute Config Keys

**Issue:** `spark.conf.get("catalog", "default")` throws `CONFIG_NOT_AVAILABLE` on serverless compute, even with a default value specified.

**Cause:** Serverless compute doesn't support arbitrary custom Spark config keys. Only built-in keys work.

**Fix:** Wrap in try/except:
```python
try:
    CATALOG = spark.conf.get("catalog")
except Exception:
    CATALOG = "cjc_aws_workspace_catalog"
```

**Rule:** Always use try/except for custom spark.conf keys in notebooks that may run on serverless.
