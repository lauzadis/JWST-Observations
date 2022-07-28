# James Webb Space Telescope Observation Tracker

This is a tracking tool for the James Webb Space Telescope. Using [observation schedules published by the Space Telescope Science Institute](https://www.stsci.edu/jwst/science-execution/observing-schedules), it will query for new observation events and send tweets when one is occurring.

https://twitter.com/JWSTObservation

## Usage

0. Clone the repository to your machine.
1. Install the necessary dependencies with `pip install .`
2. Set up your .env file with Twitter credentials (specifically, 4 values are required, the API key/secret and access token/secret)
3. Run the tool using `python3 src/main.py`