import os
from flask import Flask, request, abort

from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, TextMessage,
    TemplateMessage, ButtonsTemplate, CarouselTemplate, CarouselColumn,
    MessageAction, URIAction
)
import requests, re, time, pandas as pd
from bs4 import BeautifulSoup
import google.generativeai as genai

app = Flask(__name__)

configuration = Configuration(access_token=os.getenv('Line_channel_token'))
line_handler = WebhookHandler(os.getenv('line_channel_secret'))
genai.configure(api_key=os.getenv("google_API_KEY"))
model = genai.GenerativeModel("gemini-2.0-flash-exp")


user_states = {} 


def call_gemini_with_throttle(prompt, files=None):
    if files is None:
        files = []
    time.sleep(0.6)  
    return model.generate_content([prompt] + files)  

def crawl_financial_data(stock_id):

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'Cookie': 'CLIENT%5FID=20250424145048398%5F72%2E14%2E201%2E169; _ga=GA1.1.1250369642.1745477458; _cc_id=cef04214d728af12439013890d2f3086; IS_TOUCH_DEVICE=F; SCREEN_SIZE=WIDTH=1536&HEIGHT=864; TW_STOCK_BROWSE_LIST=2330%7C4162%7C6625; panoramaId_expiry=1748594103124; panoramaId=de27ed2b905a000c7436a45c40d54945a702fa1de5b0702b607c20e3087bed50; panoramaIdType=panoIndiv; cto_bundle=35GF7F84ZWtaTTBGNU1BNlZNR25jMHhRbWZSV2hMQlNuRkhDekxCbFh3cGZodEJYRE5TSDJ1aERja200VGVBMEZkSWprWHV1Y0lHU1ZnQVE0bktwWUNSMjNHU0dReVZXbTN2SWRRJTJGbDZYMFQ5RTNGTlFUMjlEN1pXT2dVZlYwUlkxOTRuWCUyRjhkWktqRTNNSzFMalFmRDVlZXVDJTJGWlZrVnJkNzhJQ0t2SnM3TlM2UTE5WW45aExhMXcwWHhNUVJyd1ZVU3JzTWlCMzNWcE1QZFp6SHVueDRHRmVBVTZ2TWJiTGZwaW1Td2NZYXdZdnVjVWxHTTJVOGklMkZRVERrRWREaktWUkNFSlI; FCNEC=%5B%5B%22AKsRol-apmRrzB2uInpu5UDmPF03GCxYycrkySAALLCuRnT--AIdPMNv1guFECNkVyWqnbNZ2FucYm9tv9yHcwl_LeXaYZ7e-vr6hdfb0U0QElTtlLU4_bf2SiEnfJKpaeu3CH7p8xjf4x1KD5WQS5hk_jrScDtjWg%3D%3D%22%5D%5D; _ga_0LP5MLQS7E=GS2.1.s1748020303$o18$g1$t1748020616$j55$l0$h0$dxhQGwmCJk4PMX15OnXaza1sA8mYEOrWDCw'
    }

    def fetch_balance_sheet_with_cookie(stock_id: str):
        url = f'https://goodinfo.tw/tw/StockFinDetail.asp?RPT_CAT=BS_M_QUAR&STOCK_ID={stock_id}'
        res = requests.get(url, headers=headers)
        res.encoding = 'utf-8'

        soup = BeautifulSoup(res.text, 'lxml')
        table = soup.find("table", id="tblFinDetail")
        if not table:
            print("⚠️ 找不到表格")
            return None

        rows = table.find_all("tr")
        data = []
        for row in rows:
            cols = row.find_all(['th', 'td'])
            row_data = [col.get_text(strip=True).replace('\xa0', '') for col in cols]
            data.append(row_data)

        df = pd.DataFrame(data)

        if len(df) > 2:
            quarter_row = df.iloc[0].fillna('').tolist()
            type_row = df.iloc[1].fillna('').tolist()
            new_columns = ['項目']
            for i in range(1, len(quarter_row)):
                q_clean = quarter_row[i].replace(' ', '').replace('\xa0', '').strip()
                if q_clean:
                    new_columns.append(f'{q_clean}_金額')
                    new_columns.append(f'{q_clean}_%')
            new_columns = new_columns[:len(df.columns)]
            df.columns = new_columns
            df = df[2:]

            df = df[~df['項目'].isin(['負債', '股東權益', '金額'])]
        else:
            print("⚠️ 表格行數不足無法處理欄位名稱")
            return None

        path = f"/content/{stock_id}_資產負債表.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return path



    def fetch_income_statement(stock_id: str): 
        url = f'https://goodinfo.tw/tw/StockFinDetail.asp?RPT_CAT=IS_M_QUAR_ACC&STOCK_ID={stock_id}'
        res = requests.get(url, headers=headers)
        res.encoding = 'utf-8'

        soup = BeautifulSoup(res.text, 'lxml')
        table = soup.find("table", id="tblFinDetail")
        if not table:
            print("⚠️ 找不到表格")
            return None

        rows = table.find_all("tr")
        data = []
        for row in rows:
            cols = row.find_all(['th', 'td'])
            row_data = [col.get_text(strip=True).replace('\xa0', '') for col in cols]
            data.append(row_data)

        df = pd.DataFrame(data)

        if len(df) > 2:
            quarter_row = df.iloc[0].fillna('').tolist()
            type_row = df.iloc[1].fillna('').tolist()
            new_columns = ['項目']
            for i in range(1, len(quarter_row)):
                q_clean = quarter_row[i].replace(' ', '').replace('\xa0', '').strip()
                if q_clean:
                    new_columns.append(f'{q_clean}_金額')
                    new_columns.append(f'{q_clean}_%')
            new_columns = new_columns[:len(df.columns)]
            df.columns = new_columns
            df = df[2:]


            df = df[~df['項目'].isin(['業外損益', '淨損益', '金額'])]
        else:
            print("⚠️ 表格行數不足無法處理欄位名稱")
            return None
        path = f"/content/{stock_id}_損益表.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return path



    def fetch_cashflow_sheet(stock_id: str): 
        url = f'https://goodinfo.tw/tw/StockFinDetail.asp?RPT_CAT=CF_M_QUAR&STOCK_ID={stock_id}'
        res = requests.get(url, headers=headers)
        res.encoding = 'utf-8'

        soup = BeautifulSoup(res.text, 'lxml')
        table = soup.find("table", id="tblFinDetail")
        if not table:
            print("⚠️ 找不到表格")
            return None

        rows = table.find_all("tr")
        data = []
        for row in rows:
            cols = row.find_all(['th', 'td'])
            row_data = [col.get_text(strip=True).replace('\xa0', '') for col in cols]
            data.append(row_data)

        df = pd.DataFrame(data)

        if len(df) > 2:
            df.columns = df.iloc[0]  
            df = df[1:].reset_index(drop=True)  
            df = df[df[df.columns[0]] != '金額']
        else:
            print(" 表格行數不足無法處理欄位名稱")
            return None
        path = f"/content/{stock_id}_現金流量表.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return path



    def fetch_ratio_sheet(stock_id: str): 
        url = f'https://goodinfo.tw/tw/StockFinDetail.asp?RPT_CAT=XX_M_QUAR_ACC&STOCK_ID={stock_id}'
        res = requests.get(url, headers=headers)
        res.encoding = 'utf-8'

        soup = BeautifulSoup(res.text, 'lxml')
        table = soup.find("table", id="tblFinDetail")
        if not table:
            print(" 找不到表格")
            return None

        rows = table.find_all("tr")
        data = []
        for row in rows:
            cols = row.find_all(['th', 'td'])
            row_data = [col.get_text(strip=True).replace('\xa0', '') for col in cols]
            data.append(row_data)

        df = pd.DataFrame(data)

        if len(df) > 2:
            df.columns = df.iloc[0]  
            df = df[1:].reset_index(drop=True)  
            df = df[df[df.columns[0]] != '金額']  
        else:
            print(" 表格行數不足無法處理欄位名稱")
            return None
        path = f"/content/{stock_id}_財務比率表.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return path


    def crawl_all_statements(stock_id):
        files = []
        for func in [
            fetch_balance_sheet_with_cookie, 
            fetch_income_statement,
            fetch_cashflow_sheet,
            fetch_ratio_sheet
        ]:
            result = func(stock_id)
            if result:
                files.append(result)
        return files

    return crawl_all_statements(stock_id) # Call the inner function

@app.route('/', methods=['GET'])
def index(): return 'hello!'

@app.route('/callback', methods=['POST'])
def callback():
    signature = request.headers.get('X-Line-Signature','')
    body = request.get_data(as_text=True)
    try: line_handler.handle(body, signature)
    except InvalidSignatureError: abort(400)
    return 'OK'

@line_handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id
    text = event.message.text.strip()
    mode = user_states.get(user_id)  # None, 'analyze', 'find'
    reply_msgs = []

    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)

        if text.startswith("分析:"):
            _, stock_id, topic = text.split(":")
            paths = crawl_financial_data(stock_id)
            uploaded = [genai.upload_file(p) for p in paths]
            
            prompts = {
                    'summary': (
                        "根據以下四份報表，請用條列方式回答：\n"
                        "1. 最近四季的營收、毛利、稅後淨利是否穩定？\n"
                        "2. 哪一季表現最好？哪一季最差？原因可能為何？\n"
                        "3. 是否有異常如營業利益驟降、現金流為負？"
                    ),
                    'invest': (
                        "根據以下報表，請協助分析這家公司的投資價值：\n"
                        "- ROE、ROA 與 EPS 成長一致性？\n"
                        "- 毛利率變化與競爭力指標\n"
                        "- 請給出具體投資建議"
                    ),
                    'operation': (
                        "根據以下報表，請協助分析這家公司的經營能力：\n"
                        "- 存貨週轉率、應收帳款週轉率\n"
                        "- 總資產週轉率"
                    ),
                    'solvency': (
                        "根據以下報表，請協助分析這家公司的償債能力：\n"
                        "- 流動比、速動比\n"
                        "- 負債比與利息保障倍數"
                    ),
                    'profitability': (
                        "根據以下報表，請協助分析這家公司的獲利能力：\n"
                        "- 毛利率、營業利益率、淨利率\n"
                        "- ROE、ROA 及 EPS 表現"
                    )
                }
            resp = call_gemini_with_throttle(prompts.get(topic, "請選擇有效分析項目。"), uploaded)
            line_bot_api.reply_message(
              ReplyMessageRequest(
                  reply_token=event.reply_token,
                  messages=[TextMessage(text=resp.text)]
                )
            )
            user_states[user_id] = None
            return

        if mode is None and text not in ["功能:分析", "功能:找尋"]:
            reply_msgs.append(
                TemplateMessage(
                    alt_text="主選單",
                    template=ButtonsTemplate(
                        title="今天想使用什麼服務？",
                        text="請選擇：",
                        actions=[
                            MessageAction(label="📊 分析財報", text="功能:分析"),
                            MessageAction(label="🔍 找尋財報", text="功能:找尋")
                        ]
                    )
                )
            )

            line_bot_api.reply_message(ReplyMessageRequest(reply_token=event.reply_token, messages=reply_msgs))
            return

        if text == "功能:分析":
            user_states[user_id] = 'analyze'
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text="請輸入公司代號（如2330）：")]
                )
            )
            return
        elif text == "功能:找尋":
            user_states[user_id] = 'find'
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text="請輸入公司代號（如2330）：")]
                )
            )
            return

        if mode == 'analyze' and re.fullmatch(r"\d+", text):
            stock_id = text
            paths = crawl_financial_data(stock_id)
            if not paths:
                reply = TextMessage(text=f"❌ 找不到 {stock_id} 的資料，請確認代號")
                line_bot_api.reply_message(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=[reply])
                )
            else:

                carousel = TemplateMessage(
                    alt_text="請選擇要分析的項目",
                    template=CarouselTemplate(columns=[
                        CarouselColumn(
                            thumbnail_image_url="https://kscthinktank.com.tw/wp-content/uploads/2023/11/22-1024x576.png",
                            title="財務摘要",
                            text="查看最近四季摘要",
                            actions=[MessageAction(label="📊 Summary", text=f"分析:{stock_id}:summary")]
                        ),
                        CarouselColumn(
                            thumbnail_image_url="https://bank.sinopac.com/sinopacBT/webevents/FinancialManagement/userfiles/article/058pic2.jpg",
                            title="投資建議",
                            text="評估投資價值",
                            actions=[MessageAction(label="💡 Invest", text=f"分析:{stock_id}:invest")]
                        ),
                        CarouselColumn(
                            thumbnail_image_url="https://expup.com/tw/wp-content/uploads/2019/11/Platformphoto-01-1.png",
                            title="經營能力",
                            text="分析營運指標",
                            actions=[MessageAction(label="⚙️ Operation", text=f"分析:{stock_id}:operation")]
                        ),
                        CarouselColumn(
                            thumbnail_image_url="https://att.kuaiji.com/edit/image/202008/1597219698908474.jpg",
                            title="償債能力",
                            text="分析償債相關",
                            actions=[MessageAction(label="💰 Solvency", text=f"分析:{stock_id}:solvency")]
                        ),
                        CarouselColumn(
                            thumbnail_image_url="https://ism.bwnet.com.tw/image/pool/sm/2019/01/1355c282483d9a4bb0a6bef9eac0030a.jpg",
                            title="獲利能力",
                            text="分析獲利指標",
                            actions=[MessageAction(label="📈 Profit", text=f"分析:{stock_id}:profitability")]
                        )
                    ])
                )
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[TextMessage(text=f"✅ {stock_id} 的財報已取得，請選擇分析："), carousel]
                    )
                )
            user_states[user_id] = None
            return

        if mode == 'find' and re.fullmatch(r"\d+", text):
            stock_id = text
            buttons = TemplateMessage(
                alt_text="選擇報表",
                template=ButtonsTemplate(
                    title=f"{stock_id} 報表選擇",
                    text="請點選：",
                    actions=[
                            URIAction(label="📋 資產負債表",
                                      uri=f"https://goodinfo.tw/tw/StockFinDetail.asp?RPT_CAT=BS_M_QUAR&STOCK_ID={stock_id}"),
                            URIAction(label="💹 損益表",
                                      uri=f"https://goodinfo.tw/tw/StockFinDetail.asp?RPT_CAT=IS_M_QUAR_ACC&STOCK_ID={stock_id}"),
                            URIAction(label="💰 現金流量表",
                                      uri=f"https://goodinfo.tw/tw/StockFinDetail.asp?RPT_CAT=CF_M_QUAR&STOCK_ID={stock_id}")
                        ]
                    )
                )

            line_bot_api.reply_message(
                ReplyMessageRequest(reply_token=event.reply_token, messages=[buttons])
            )
            user_states[user_id] = None
            return


        user_states[user_id] = None
        buttons = TemplateMessage(
            alt_text="主選單",
            template=ButtonsTemplate(
                title="無效輸入，請重新選擇功能。",
                text="今天想使用什麼服務？",
                actions=[
                    MessageAction(label="📊 分析財報", text="功能:分析"),
                    MessageAction(label="🔍 找尋財報", text="功能:找尋")
                ]
            )
        )
        line_bot_api.reply_message(
            ReplyMessageRequest(reply_token=event.reply_token, messages=[buttons])
        )
