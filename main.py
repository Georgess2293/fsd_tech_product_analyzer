import hook
import prehook
import posthook
import time
import datetime
import praw


def etl_job(input_text,reddit):
    prehook.execute_prehook(reddit)
    hook.execute_hook(input_text,reddit)
    posthook.execute_posthook()

input_text="Xiaomi 13T pro"
reddit=praw.Reddit(
            client_id="your_client_id",
            client_secret="your_client_secret",
            user_agent="your_user_agent"
        )

etl_job("Xiaomi 13T Pro",reddit)
    

