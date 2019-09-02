# Yelp Scio

## Raison d'être:

This is a playground for me to fiddle with Scio.

It uses [a Yelp sample dataset](https://www.yelp.com/dataset/) ([documentation](https://www.yelp.com/dataset/documentation/main)).

## Roadmap

- [ ] Happy path of an analysis of open businesses, median and p95 opening and closing times.
- [ ] Happy path pieces testing.
- [ ] Local run.
- [ ] Dataflow run.
- [ ] Make code failures observable. Example: add more wrong data and check that you can see the broken line.
- [ ] Review and optimize performance.
- [ ] Mock x10 data and check that aggregation scales properly.
- [ ] Better output formats:
  - [ ] SQLite for better typing and interoperability.
  - [ ] Parquet or Avro for performance.

## Features:

This project comes with number of preconfigured features, including:

### sbt-pack

Use `sbt-pack` instead of `sbt-assembly` to:
 * reduce build time
 * enable efficient dependency caching
 * reduce job submission time

To build package run:

```
sbt pack
```

### Testing

This template comes with an example of a test, to run tests:

```
sbt test
```

### Scala style

Find style configuration in `scalastyle-config.xml`. To enforce style run:

```
sbt scalastyle
```

### REPL

To experiment with current codebase in [Scio REPL](https://github.com/spotify/scio/wiki/Scio-REPL)
simply:

```
sbt repl/run
```

---

This project is based on the [scio.g8](https://github.com/spotify/scio.g8).
