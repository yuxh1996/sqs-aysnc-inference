import requests
from scheduler.conf import schedulerConfig

# Get service port from configuration
service_port = schedulerConfig.get('service', 'service_port')

api_url = f"http://127.0.0.1:{service_port}"

def process_request(api, taskInfo):
    """
    Process a given task by calling a specified API with task information.

    Args:
        api (str): The API endpoint to call.
        taskInfo (dict): The information of the task to process.

    Returns:
        dict: The response from the API call.
    """
    print(f"Processing task: {taskInfo}")
    # Uncomment if taskInfo is a JSON string that needs to be parsed
    # task = json.loads(taskInfo)
    res = call_simple_api(api, taskInfo)
    return res

def call_simple_api(api, payload):
    """
    Call a simple API with a given payload.

    Args:
        api (str): The API endpoint to call.
        payload (dict): The payload to send in the API request.

    Returns:
        dict: The response from the API call or an error message.
    """
    print(f"Calling API: {api} with payload: {payload}")
    try:
        response = requests.post(url=f'{api_url}{api}', json=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return {"error": str(e)}
