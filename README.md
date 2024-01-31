# Airbyte destination

This destination follows the [Airbyte Protocol](https://docs.airbyte.com/understanding-airbyte/airbyte-protocol). 
The protocol offers an introduction to Airbyte concepts, message types and format, plus a detailed explanation of the 3 commands the destination should implement: `spec`, `check`, and `write`.

## Running each command

To build the docker image, first run:
```shell
make
```

```shell
docker build . -t airbyte/destination-propel
```

### Spec
```shell
docker run --rm airbyte/destination-propel spec
```

### Check
First, set up your secrets config file with the Propel App ID and secret.
```shell
make secrets APP_ID=<Application ID> SECRET=<Application secret>
```
Now with the `secrets/config.json` file all set up you can run:
```shell
docker run -v $(pwd)/secrets:/secrets --rm airbyte/destination-propel check --config /secrets/config.json
```

### Write
If you haven't already, set up the secrets config file as described in the command above.
To test the `write` command you should pass down:
- The config file previously generated.
- A configured catalog. It describes an Airbyte Stream, which is a Data Pool on our side.
- Record and State messages via stdin. These are the records that will be inserted to the Data Pool.

Samples of the last two can be found in `sample_files/` and can be used as such:
```shell
docker run --rm -i -v $(pwd)/secrets:/secrets -v $(pwd)/sample_files:/sample_files airbyte/destination-propel write --config /secrets/config.json --catalog /sample_files/configured_catalog.json < sample_files/input_data.txt
```