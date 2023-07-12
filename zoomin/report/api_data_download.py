import os
import traceback
from dotenv import load_dotenv, find_dotenv
from zoomin_client import client

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()
# load up the entries as environment variables
load_dotenv(dotenv_path)

api_key = os.environ.get("DJANGO_API_Key")

SAVE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "output", "api_data"
)


def download_variable_data(values):
    variable, country_code = values

    try:
        response_data = client.get_variable_data(
            api_key=api_key,
            variable_name=variable,
            spatial_resolution="LAU",
            country_code=country_code,
            result_format="df",
            save_result=True,
            save_path=SAVE_PATH,
            save_name=f"{country_code}_{variable}",
        )
        print(f"worked on {country_code}_{variable}")
    except:
        print(f"!!!job failed!!!!{traceback.format_exc()}")
