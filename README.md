# CQuirrel - AJU for TPC-H Q10

## Development

### Format Code

```bash
# Auto-format all Java files (google-java-format)
mvn spotless:apply

# Check formatting without modifying files (runs automatically in `mvn verify`)
mvn spotless:check
```

### Lint

Error Prone runs automatically during compilation:

```bash
mvn compile
```

### Full Build

```bash
# Compile + Error Prone + format check
mvn verify
```
