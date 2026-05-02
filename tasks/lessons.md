# Lessons Learned

Corrections and patterns to avoid repeating mistakes.

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
