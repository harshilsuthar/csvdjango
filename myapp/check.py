import asyncio
from django.http import JsonResponse
from asgiref.sync import sync_to_async
from time import sleep

async def crunching_stuff(time):
    sleep(time)
    print("Woke up after %s seconds!"%(time))

async def index(time):
    json_payload = {
        "message": "Hello world"
    }
    """
    or also
    asyncio.ensure_future(crunching_stuff())
    loop.create_task(crunching_stuff())
    """
    # asyncio.run(crunching_stuff(time))
    # asyncio.run(crunching_stuff(time))
    await crunching_stuff(2)
    await crunching_stuff(1)
    print('called')


# asyncio.run(index(2))
# asyncio.run(index(1))
asyncio.run(index(2))
asyncio.run(index(2))