import hook
import prehook
import posthook
import time
import datetime


def etl_job(input_text):
    print("Start time: ",datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y"))
    prehook.execute_prehook()
    hook.execute_hook(input_text)
    posthook.execute_posthook()
    print("Start time: ",datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y"))