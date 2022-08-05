import requests
import os
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta
import urllib
import pandas as pd
import time
from requests_oauthlib import OAuth1

class JWSTObservationBot():
    def __init__(self):
        self.observing_schedules_url = "https://www.stsci.edu/jwst/science-execution/observing-schedules"
        self.base_url = "https://www.stsci.edu"
        self.tweet_url = "https://api.twitter.com/2/tweets"

        self.observing_schedule = None
        self.seen_observing_schedules = set()
        if os.path.exists("jwst_observing_schedule.csv"): 
            print("Reading observing schedule from disk")
            self.observing_schedule = pd.read_csv("jwst_observing_schedule.csv", index_col="VISIT ID")
            self.observing_schedule["SCHEDULED START TIME"] = pd.to_datetime(self.observing_schedule["SCHEDULED START TIME"])
            self.observing_schedule["DURATION"] = pd.to_timedelta(self.observing_schedule["DURATION"])

            with open("jwst_seen_observing_schedules.csv") as file:
                self.seen_observing_schedules = set([schedule for schedule in file.readline().split(",")])
        self.update_observing_schedule()

        self.sleep_duration = 5  # sleep time in seconds

        self.last_saved_time = 0
        self.save_frequency = 3600  # how often the bot should save the observing schedule to disk

        self.init_environ()
        api_key = os.getenv("API_KEY")
        api_key_secret = os.getenv("API_KEY_SECRET")
        access_token = os.getenv("ACCESS_TOKEN")
        access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")

        self.oauth = OAuth1(api_key, api_key_secret, access_token, access_token_secret, signature_type="auth_header")

    def init_environ(self):
        if os.path.exists("../.env"):
            with open("../.env") as file:
                for line in file.readlines():
                    key, val = line.strip("\n").split("=")
                    os.environ[key] = val
        else:
            exit("Failed to find .env file, which is critical for making API requests. View the README for troubleshooting.")

    def parse_duration(self, duration):
        days = int(duration.split("/")[0])

        time = duration.split("/")[1].split(":")
        hours = int(time[0])
        minutes = int(time[1])
        seconds = int(time[2])
        
        delta = timedelta(hours=days*24 + hours, minutes=minutes, seconds=seconds)
        return delta

    def parse_start_time(self, start_time):
        if start_time is None:
            return None
        if "ATTACHED TO PRIME" in start_time:  # TODO items with "^ATTACHED TO PRIME^" should copy the duration from the item immediately preceding it
            return None

        return datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")

    def update_observing_schedule(self):
        r = requests.get(self.observing_schedules_url)
        soup = BeautifulSoup(r.content, features="lxml")
        links = soup.find_all("a")

        for link in links:
            href = link.get("href")
            if "/files/live/sites/www" in href:
                filepath = os.path.basename(href)
                if filepath in self.seen_observing_schedules:
                    continue
                else:
                    self.seen_observing_schedules.add(filepath)

                file_url = urllib.parse.urljoin(self.base_url, href)
                print("Processing file " + file_url)
                r = requests.get(file_url)
                lines = r.content.decode("utf-8").split("\n")
                headers = [field.strip() for field in lines[2].split("  ") if len(field) > 0]
                headers.append("seen")

                df = pd.DataFrame(columns=headers)

                for line in lines[4:]:
                    data = [field.strip() for field in line.split("  ") if len(field) > 0]

                    while len(data) < len(headers) - 1:  # in case of some missing fields in some rows (i.e VISIT TYPE of PRIME UNTARGETED has no TARGET NAME)
                        data.append(None)
                    data.append(False)

                    df.loc[len(df)] = data

                df["SCHEDULED START TIME"] = df["SCHEDULED START TIME"].apply(self.parse_start_time)
                df = df[df["SCHEDULED START TIME"].notna()]

                df["DURATION"] = df["DURATION"].apply(self.parse_duration)

                df = df.sort_values("SCHEDULED START TIME", axis=0)[::-1]
                df = df.set_index("VISIT ID")
                print(self.observing_schedule)

                if self.observing_schedule is None:
                    print("overwriting nonetype")    
                    self.observing_schedule = df
                else:
                    self.observing_schedule = pd.concat([self.observing_schedule, df])
                    self.observing_schedule = self.observing_schedule.sort_values("SCHEDULED START TIME", axis=0)[::-1]

    def check_for_new_observation_event(self):
        now = datetime.utcnow()

        while self.observing_schedule is None or len(self.observing_schedule) == 0:
            self.update_observing_schedule()
            if self.observing_schedule is None or len(self.observing_schedule) == 0:
                self.sleep()
        
        for _, event in self.observing_schedule.iterrows():
            if event.seen: 
                continue

            start = event["SCHEDULED START TIME"]
            delta = event["DURATION"]
            if start < now < start + delta:
                self.alert_new_observation_event(event)

    def alert_new_observation_event(self, event):
        duration = event["DURATION"].to_pytimedelta()

        days = duration.days
        hours = duration.seconds // 3600
        minutes = duration.seconds % 3600 // 60

        day_plural = "day" if days == 1 else "days"
        hour_plural = "hour" if hours == 1 else "hours"
        minute_plural = "minute" if minutes == 1 else "minutes"

        if days == 0:
            if hours == 0:
                duration = f"{minutes} {minute_plural}"
            else:
                duration = f"{hours} {hour_plural} and {minutes} {minute_plural}"
        else:
            duration = f"{days} {day_plural} "
            if hours == 0:
                duration += f"and {minutes} {minute_plural}"
            else:
                duration += f", {hours} {hour_plural} and {minutes} {minute_plural}"

        proposal_root = "https://www.stsci.edu/jwst/phase2-public/"

        text = f"I am now observing {event['TARGET NAME']} using {event['SCIENCE INSTRUMENT AND MODE']} for {duration}. "
        text += f"Keywords: {event['KEYWORDS']}. "
        text += f"Proposal: {proposal_root + event.name.split(':')[0]}.pdf {':'.join(event.name.split(':')[1:])}"

        print(f"Tweeting: {text}")
        r = requests.post("https://api.twitter.com/2/tweets", auth=self.oauth, json={"text": text})
        
        if not r.ok:
            if r.status_code == 403 and "duplicate content" in r.json()["detail"]:
                self.observing_schedule.at[event.name, "seen"] = True
            print(f"Failed to send a tweet with text {text}. {r.content}")
        else:
            self.observing_schedule.at[event.name, "seen"] = True

    def sleep(self):
        print("Sleeping for " + str(self.sleep_duration) + ". Began at " + str(datetime.utcnow()))
        time.sleep(self.sleep_duration)
        print("Finished sleeping at " + str(datetime.utcnow()))

    def save(self):
        self.observing_schedule.to_csv("jwst_observing_schedule.csv", index=True)
        with open("jwst_seen_observing_schedules.csv", "w") as file:
            file.write(",".join(self.seen_observing_schedules))
        self.last_saved_time = time.time()

    def loop(self):
        while True:
            if time.time() - self.last_saved_time >= self.save_frequency:    
                self.update_observing_schedule()
                self.save()

            print("Checking for a new observation event")
            event = self.check_for_new_observation_event()
            if event is not None:
                print("Found new event: " + str(event))
                self.alert_new_observation_event()
            self.sleep()

if __name__ == "__main__":
    jwst = JWSTObservationBot()
    jwst.loop()