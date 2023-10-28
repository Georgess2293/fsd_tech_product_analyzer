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

# input_text="Xiaomi 13T pro"
# reddit=praw.Reddit(
#             client_id="A99udy2Ex7RaoBzW5O3Gdw",
#             client_secret="jOKXzOzOe9sk-wn-i5a7c4I4zdac4w",
#             user_agent="my-tech"
#         )

# etl_job("Xiaomi 13T Pro",reddit)
    

