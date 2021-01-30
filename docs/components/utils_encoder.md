---
sort: 2
---

# Encoder Field

Datasets in spark can be encoded (something similar to a strong typing).
A component may define an _encodedAs_ field. 
If present, the encoder will describe what format should be applied to the dataset.

## Fields

Available encoders are `value`, `tuple`, `bean` and `seralization`:

| Encoder Type | Possible Value |
| ------------ | -------------- |
| `value` | One of: `BINARY`, `BOOLEAN`, `BYTE`, `DATE`, `DECIMAL`, `DOUBLE`, `FLOAT`, `INSTANT`, `INT`, `LOCALDATE`, `LONG`, `SHORT`, `STRING`, `TIMESTAMP` |
| `tuple` | A list of 2 or more encoders |
| `bean` | The fully qualified name of a a Java class to represent the schema and type of all fields |
| `serialization` | One of: `JAVA`, `KRYO`; plus a class name. |

## Examples

Some example of an encoder in yaml representation:
```yaml
# value
{ schema: value, value: STRING }

# tuple
{ schema: tuple, of: [ { schema: value, value: STRING }, { schema: value, value: INT } ] }

# bean
{ schema: bean, ofClass: some.java.class.Bean }

# serialization
{ schema: serialization, variant: KRYO , ofClass: some.java.class.Bean }
```

Note that the default serialization is `value`, so it can be omitted:
```yaml
# same
{ value: STRING }
{ schema: value, value: STRING }

# same
{ schema: tuple, of: [ { value: STRING }, { value: INT } ] }
{ schema: tuple, of: [ { schema: value, value: STRING }, { schema: value, value: INT } ] }
```
