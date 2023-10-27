import hook
import prehook
import posthook
import time
import datetime
import praw


def etl_job(input_text,reddit):
    print("Start time: ",datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y"))
    prehook.execute_prehook()
    hook.execute_hook(input_text,reddit)
    posthook.execute_posthook()
    print("End time: ",datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y"))

# input_text="Xiaomi 13T pro"
# reddit=praw.Reddit(
#             client_id="A99udy2Ex7RaoBzW5O3Gdw",
#             client_secret="jOKXzOzOe9sk-wn-i5a7c4I4zdac4w",
#             user_agent="my-tech"
#         )

# etl_job("Xiaomi 13T Pro",reddit)
    

