# tap-exact

`tap-exact` is a Singer tap for Exact.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install tap-exact
```

## Credentials

### Create a Config file

```
{
  "client_id": "client_id",
  "client_secret": "client_secret",
  "refresh_token": "refresh_token",
  "access_token": "access_token",
  "start_date": "2000-01-01T00:00:00Z",
  "sync_endpoints": true,
  "current_division": "exact_division",
}
```

The `client_id` and `client_secret` keys are your OAuth Quickbooks App secrets. The `refresh_token` is a secret created during the OAuth flow. For more info on the Exact OAuth flow, visit the [Exact documentation](https://support.exactonline.com/community/s/knowledge-base#All-All-DNO-Content-oauth-eol-oauth-dev-impleovervw).

The `start_date` is used by the tap to fetch records from that date on.  This should be an [RFC3339](https://www.ietf.org/rfc/rfc3339.txt) formatted date-time, like "2018-01-08T00:00:00Z". For more details, see the [Singer best practices for dates](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#dates).

When `sync_endpoints`is true, the tap will use Exact's sync endpoints instead of bulk or standard endpoints for streams that support them. Using the sync endpoints is generally recommended, as it is the most efficient way to get new or changed records.

Most Exact Online REST API resource URIs require a Division parameter `current_division`. This parameter identifies the division that is accessed. For more details on how to get the current division for your credentials see the [Me endpoint](https://start.exactonline.nl/docs/HlpRestAPIResourcesDetails.aspx?name=SystemSystemMe)

## Configuration

### Accepted Config Options

- [ ] `Developer TODO:` Provide a list of config options accepted by the tap.

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-exact --about
```

### Source Authentication and Authorization

- [ ] `Developer TODO:` If your tap requires special access on the source system, or any special authentication requirements, provide those here.

## Usage

You can easily run `tap-exact` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-exact --version
tap-exact --help
tap-exact --config CONFIG --discover > ./catalog.json
```

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_exact/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-exact` CLI interface directly using `poetry run`:

```bash
poetry run tap-exact --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-exact
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-exact --version
# OR run a test `elt` pipeline:
meltano elt tap-exact target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
