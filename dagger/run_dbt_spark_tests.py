import argparse
import sys

import anyio as anyio
import dagger as dagger


async def test_spark(test_args):
    async with dagger.Connection(dagger.Config(log_output=sys.stderr)) as client:
        install_dir = client.host().directory("./", exclude=["\\.pytest_cache/*", ".idea/*"])

        tst_container = (
            client.container()
            .from_("python:3.8-slim")
            .with_directory("/dbt_spark", install_dir)
            .with_workdir("/dbt_spark")
            .with_exec("./dagger/configure_odbc.sh")
            .with_exec(["pip", "install", "-r", "requirements.txt"])
            .with_exec(["pip", "install", "-r", "dev-requirements.txt"])
        )

        result = await (tst_container
                        .with_workdir("/dbt_spark")
                        .with_exec(["python", '-m', 'pytest', '-v',
                                    '--profile', test_args.profile,
                                    '-n', 'auto',
                                    'tests/functional/']
                                   )
                        ).stdout()

        return result


parser = argparse.ArgumentParser()
parser.add_argument("--profile", required=True, type=str)
args = parser.parse_args()

anyio.run(test_spark, args)
