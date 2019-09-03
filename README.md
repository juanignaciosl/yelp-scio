# Yelp Scio

## Raison d'être:

This is a playground for me to fiddle with Scio.

It uses [a Yelp sample dataset](https://www.yelp.com/dataset/) ([documentation](https://www.yelp.com/dataset/documentation/main)).

## Roadmap

- [X] Happy path of an analysis of open businesses, median and p95 opening and closing times.
- [X] Happy path for coolness joins.
- [X] Happy path pieces testing.
- [X] Local run.
- [X] Dataflow run.
- [ ] Merge output files?
- [ ] Make code failures observable. Example: add more wrong data and check that you can see the broken line.
- [ ] Review and optimize performance.
- [ ] Mock x10 data and check that aggregation scales properly.
- [ ] Better output formats:
  - [ ] SQLite for better typing and interoperability.
  - [ ] Parquet or Avro for performance.
- Refactor:
  - [ ] Remove Exception throwing.

## Running

### Local

Default output is at `/tmp/yelp-scio`.

```bash
$ export SBT_OPTS="-Xmx8G -Xms8G -Xss1M -XX:MaxMetaspaceSize=1G -XX:+CMSClassUnloadingEnabled -XX:ReservedCodeCacheSize=128m"
$ sbt
sbt:yelp-scio> runMain juanignaciosl.yelp.YelpBusinessesRunner --input=<path-to-yelp-data-dir> --output=<output-path-such-as-/tmp/yelp-scio-run>
```

### Google Dataflow

Check [Setting Up Authentication for Server to Server Production Applications](https://developers.google.com/accounts/docs/application-default-credentials).

The credentials needs access to:
- Dataflow.
- Google Storage buckets with the data.
- Cloud Resource Manager API.

Note: `-o GSUtil:parallel_composite_upload_threshold=150M` improves upload speed a lot ;-)

Uploading files: `gsutil -o GSUtil:parallel_composite_upload_threshold=150M cp ./business.zip gs://yelp-scio/input`.

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS=
$ export SBT_OPTS="-Xmx4G -Xms4G -Xss1M -XX:MaxMetaspaceSize=1G -XX:+CMSClassUnloadingEnabled -XX:ReservedCodeCacheSize=128m"
$ sbt
sbt:yelp-scio> runMain juanignaciosl.yelp.YelpBusinessesRunner --project=yelp-scio --zone=us-east1-b --runner=DataflowRunner --input=gs://yelp-scio/input --output=gs://yelp-scio/output
```

You can download the output with this command: `gsutil cp -r gs://yelp-scio/output . `.

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
