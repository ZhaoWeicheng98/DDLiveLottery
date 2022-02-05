# -*- coding: utf-8 -*-
import asyncio
import random
import json
import sys
import jsonlines
import time
import blivedm
import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from backports.zoneinfo import ZoneInfo

CST = ZoneInfo("Asia/Shanghai")

async def run_single_client(room_id):
    """
    演示监听一个直播间
    """
    # 如果SSL验证失败就把ssl设为False，B站真的有过忘续证书的情况
    client = blivedm.BLiveClient(room_id, ssl=True)
    handler = MyHandler(room_id=room_id)
    client.add_handler(handler)

    client.start()


class MyHandler(blivedm.BaseHandler):
    # # 演示如何添加自定义回调
    # _CMD_CALLBACK_DICT = blivedm.BaseHandler._CMD_CALLBACK_DICT.copy()
    #
    # # 入场消息回调
    # async def __interact_word_callback(self, client: blivedm.BLiveClient, command: dict):
    #     print(f"[{client.room_id}] INTERACT_WORD: self_type={type(self).__name__}, room_id={client.room_id},"
    #           f" uname={command['data']['uname']}")
    # _CMD_CALLBACK_DICT['INTERACT_WORD'] = __interact_word_callback  # noqa

    def __init__(self,room_id) -> None:
        super().__init__()
        self.room_id = room_id

    async def _on_heartbeat(self, client: blivedm.BLiveClient, message: blivedm.HeartbeatMessage):
        with jsonlines.open(f'heartbeat_{self.room_id}.jsonl', mode='a') as writer:
            writer.write({"popularity": message.popularity})
        # print(f'[{client.room_id}] 当前人气值：{message.popularity}')

    async def _on_danmaku(self, client: blivedm.BLiveClient, message: blivedm.DanmakuMessage):
        with jsonlines.open(f'danmuku_{self.room_id}.jsonl', mode='a') as writer:
            writer.write({"timestamp": message.timestamp,
                          "uid": message.uid,
                          "uname": message.uname,
                          "msg": message.msg})
        # print(f'[{client.room_id}] {message.uname}：{message.msg}')

    async def _on_gift(self, client: blivedm.BLiveClient, message: blivedm.GiftMessage):
        with jsonlines.open(f'gift_{self.room_id}.jsonl', mode='a') as writer:
            writer.write({"timestamp": message.timestamp,
                          "uid": message.uid,
                          "uname": message.uname,
                          "gift_name": message.gift_name,
                          "num": message.num,
                          "coin_type": message.coin_type,
                          "total_coin": message.total_coin})
        # print(f'[{client.room_id}] {message.uname} 赠送{message.gift_name}x{message.num}'
        #       f' （{message.coin_type}瓜子x{message.total_coin}）')

    async def _on_buy_guard(self, client: blivedm.BLiveClient, message: blivedm.GuardBuyMessage):
        with jsonlines.open(f'guard_{self.room_id}.jsonl', mode='a') as writer:
            writer.write({"timestamp": message.timestamp,
                          "uid": message.uid,
                          "uname": message.username,
                          "gift_name": message.gift_name,
                          "num": message.num,
                          "guard_level": message.guard_level,
                          "price": message.price})
        # print(f'[{client.room_id}] {message.username} 购买{message.gift_name}')

    async def _on_super_chat(self, client: blivedm.BLiveClient, message: blivedm.SuperChatMessage):
        with jsonlines.open(f'superchat_{self.room_id}.jsonl', mode='a') as writer:
            writer.write({"timestamp": message.timestamp,
                          "uid": message.uid,
                          "uname": message.uname,
                          "message": message.message,
                          "price": message.price})
        # print(
        #     f'[{client.room_id}] 醒目留言 ¥{message.price} {message.uname}：{message.message}')


async def danmuku_lottery(room_id, conf):
    start_time = datetime.datetime.strptime(conf['start_time'],"%Y-%m-%d %H:%M:%S").replace(tzinfo=CST)
    end_time = start_time + datetime.timedelta(seconds=int(conf['duration']))

    valid_list = []

    with jsonlines.open(f'danmuku_{room_id}.jsonl', "r") as reader:
        for obj in reader:
            ttime = datetime.datetime.fromtimestamp(int(obj['timestamp'])//1000,tz=CST)
            if ttime < start_time or ttime > end_time:
                continue
            
            if conf['cond_type'] == "danmuku_equal" and obj['msg'] != conf['danmuku_content']:
                continue

            if conf['cond_type'] == "danmuku_contain" and obj['msg'].find(conf['danmuku_content']) == -1:
                continue

            valid_list.append(obj)

    if conf['only_once'] is True:
          valid_list = list({r['uid']: r for r in valid_list}.values())

    if len(valid_list) > int(conf['prize_num']):
        lot_list = random.sample(valid_list,k=int(conf['prize_num']))
    else:
        lot_list = valid_list

    with open(f'result_{room_id}_{int(time.time())}.txt',"w",encoding="utf-8") as f:
        f.write(f'抽奖时间：{datetime.datetime.now()}'+'\n')
        f.write(f'抽奖信息：{json.dumps(conf,ensure_ascii=False)}'+'\n')
        f.write(f'有效记录条数：{len(valid_list)}'+'\n')
        f.write(f'中奖用户信息:{json.dumps(lot_list,ensure_ascii=False)}'+'\n\n')
        f.write(f'全部有效记录:{json.dumps(valid_list,ensure_ascii=False)}'+'\n')
    
    display_text = f"  恭喜以下用户获得{conf['prize_name']}*1："
    for obj in lot_list:
        display_text = display_text + f"{obj['uname']} "
    
    display_count = int(conf['display_time'])
    while display_count >= 0:
        with open(f'obs_text_source.txt','w',encoding="utf-8") as f:
            f.write(display_text)
        print(display_text)
        await asyncio.sleep(1.0)
        display_count -= 1
    with open(f'obs_text_source.txt','w',encoding="utf-8") as f:
        f.write("")
            

async def gift_lottery(room_id, conf):
    start_time = datetime.datetime.strptime(conf['start_time'],"%Y-%m-%d %H:%M:%S").replace(tzinfo=CST)
    end_time = start_time + datetime.timedelta(seconds=int(conf['duration']))

    valid_list = []

    with jsonlines.open(f'gift_{room_id}.jsonl', "r") as reader:
        for obj in reader:
            ttime = datetime.datetime.fromtimestamp(int(obj['timestamp']),tz=CST)
            if ttime < start_time or ttime > end_time:
                continue
            
            if conf['cond_type'] == "gift" and obj['gift_name'] != conf['gift_name']:
                continue

            valid_list.append(obj)

    if conf['only_once'] is True:
          valid_list = list({r['uid']: r for r in valid_list}.values())

    if len(valid_list) > int(conf['prize_num']):
        lot_list = random.sample(valid_list,k=int(conf['prize_num']))
    else:
        lot_list = valid_list

    with open(f'result_{room_id}_{int(time.time())}.txt',"w",encoding="utf-8") as f:
        f.write(f'抽奖时间：{datetime.datetime.now()}'+'\n')
        f.write(f'抽奖信息：{json.dumps(conf,ensure_ascii=False)}'+'\n')
        f.write(f'有效记录条数：{len(valid_list)}'+'\n')
        f.write(f'中奖用户信息:{json.dumps(lot_list,ensure_ascii=False)}'+'\n\n')
        f.write(f'全部有效记录:{json.dumps(valid_list,ensure_ascii=False)}'+'\n')
    
    display_text = f"  恭喜以下用户获得{conf['prize_name']}*1："
    for obj in lot_list:
        display_text = display_text + f"{obj['uname']} "
    
    display_count = int(conf['display_time'])
    while display_count >= 0:
        with open(f'obs_text_source.txt','w',encoding="utf-8") as f:
            f.write(display_text)
        print(display_text)
        await asyncio.sleep(1.0)
        display_count -= 1
    with open(f'obs_text_source.txt','w',encoding="utf-8") as f:
        f.write("")

async def display_lottery_text(conf):
    if conf['cond_type'] == "danmuku_equal":
        display_text = f"  发送弹幕\"{conf['danmuku_content']}\" 即可参与抽奖，奖品为 \"{conf['prize_name']}\"*1，共{conf['prize_num']}份，剩余时间："
    elif conf['cond_type'] == "danmuku_contain":
        display_text = f"  发送包含\"{conf['danmuku_content']}\"的祝福弹幕 即可参与抽奖，奖品为 \"{conf['prize_name']}\"*1，共{conf['prize_num']}份，剩余时间："
    elif conf['cond_type'] == "gift":
        display_text = f"  赠送礼物\"{conf['gift_name']}\"*1 即可参与抽奖，奖品为 \"{conf['prize_name']}\"*1，共{conf['prize_num']}份，剩余时间："
    
    
    display_count = int(conf['duration'])
    while display_count >= 0:
        time_text = str(datetime.timedelta(seconds=display_count))
        with open(f'obs_text_source.txt','w',encoding="utf-8") as f:
            f.write(display_text+time_text)
        print(display_text+time_text)
        await asyncio.sleep(1.0)
        display_count -= 1

if __name__ == '__main__':
    with open("config.json", "r", encoding="utf-8") as f:
        config = json.load(f)
    if config is None:
        sys.exit(1)

    room_id = config["room_id"]
    scheduler = AsyncIOScheduler()
    for lot in config["lotteries"]:
        start_time = datetime.datetime.strptime(lot['start_time'],"%Y-%m-%d %H:%M:%S").replace(tzinfo=CST)
        end_time = start_time + datetime.timedelta(seconds=int(lot['duration']))
        scheduler.add_job(display_lottery_text, 'date', run_date=start_time, args=[lot])
        if lot['cond_type'] == "danmuku_equal" or lot['cond_type'] == "danmuku_contain":
            scheduler.add_job(danmuku_lottery, 'date', run_date=end_time, args=[room_id,lot])
        elif lot['cond_type'] == "gift":
            scheduler.add_job(gift_lottery, 'date', run_date=end_time, args=[room_id,lot])
    scheduler.start()
    loop = asyncio.get_event_loop()
    loop.create_task(run_single_client(room_id))
    loop.run_forever()