import asyncio
import json
import os
import requests
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# === 설정 ===
API_TOKEN = '7223058846:AAHEyqjmMLt1DkuDb5RYMIomBOVY_1pD3pE'
RPC_ENDPOINT = 'https://mainnet.helius-rpc.com/?api-key=1b9e5b88-5027-4f16-9616-2e9c1acf840e'
USER_DB = 'users.json'

bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# === 상태 관리 ===
class RegisterState(StatesGroup):
    waiting_for_wallet = State()

class ChangeWalletState(StatesGroup):
    waiting_for_new_wallet = State()

# === 파일 입출력 ===
def load_users():
    if not os.path.exists(USER_DB):
        return {}
    with open(USER_DB, 'r') as f:
        return json.load(f)

def save_users(users):
    with open(USER_DB, 'w') as f:
        json.dump(users, f, indent=2)

# === Solana 정보 조회 ===
def get_sol_balance(wallet_address):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [wallet_address]
    }
    try:
        response = requests.post(RPC_ENDPOINT, json=payload)
        data = response.json()
        lamports = data['result']['value']
        sol = lamports / 1_000_000_000
        return sol
    except Exception as e:
        return f"에러: {e}"

def get_token_accounts(wallet_address):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenAccountsByOwner",
        "params": [
            wallet_address,
            {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
            {"encoding": "jsonParsed"}
        ]
    }
    try:
        response = requests.post(RPC_ENDPOINT, json=payload)
        data = response.json()
        tokens = []
        for acc in data['result']['value']:
            amount = acc['account']['data']['parsed']['info']['tokenAmount']['uiAmount']
            mint = acc['account']['data']['parsed']['info']['mint']
            if amount and amount > 0:
                tokens.append((mint, amount))
        return tokens
    except Exception as e:
        return []

# === 메뉴 표시 ===
async def show_menu(message, user_id):
    users = load_users()

    if user_id in users:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📄 Status 확인", callback_data="status")],
            [InlineKeyboardButton(text="🔄 지갑주소 변경", callback_data="change_wallet")],
            [InlineKeyboardButton(text="🪙 자동 밈코인 투자하기", callback_data="auto_meme")]
        ])

        await message.answer("📌 메뉴를 선택해주세요:", reply_markup=keyboard)
    else:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ 가입하기", callback_data="register")]
        ])
        await message.answer("👋 아직 가입되어 있지 않습니다. 가입하시겠습니까?", reply_markup=keyboard)

# === 핸들러 ===
@dp.message(Command('start'))
async def start(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    await show_menu(message, user_id)

@dp.message(Command('menu'))
async def menu(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    await show_menu(message, user_id)

@dp.callback_query(lambda call: call.data == "register")
async def ask_wallet(call: CallbackQuery, state: FSMContext):
    await call.message.answer("🪙 솔라나 지갑 주소를 입력해주세요:")
    await state.set_state(RegisterState.waiting_for_wallet)
    await call.answer()

@dp.message(RegisterState.waiting_for_wallet)
async def save_wallet(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    users = load_users()
    wallet = message.text

    if not wallet.startswith('0x') and len(wallet) < 20:
        await message.answer("❗️올바른 지갑 주소를 입력해주세요.")
        return

    users[user_id] = {"wallet": wallet}
    save_users(users)

    await message.answer(f"🎉 가입이 완료되었습니다!\n등록된 지갑 주소: {wallet}")
    await state.clear()
    await show_menu(message, user_id)

@dp.callback_query(lambda call: call.data == "status")
async def show_status(call: CallbackQuery):
    user_id = str(call.from_user.id)
    users = load_users()

    if user_id in users:
        wallet = users[user_id]['wallet']
        sol_balance = get_sol_balance(wallet)
        tokens = get_token_accounts(wallet)

        message = f"📄 현재 상태:\n✅ 가입됨\n🪙 지갑 주소: {wallet}\n\n"
        message += f"💰 SOL 잔액: {sol_balance}\n"

        if tokens:
            message += "\n📦 보유 SPL 토큰:\n"
            for mint, amount in tokens:
                message += f"• {mint[:4]}...{mint[-4:]} : {amount}\n"
        else:
            message += "\n📦 보유 SPL 토큰 없음"

        await call.message.answer(message)
    else:
        await call.message.answer("❗️ 아직 가입되어 있지 않습니다. /start 로 가입할 수 있습니다.")
    await call.answer()

@dp.callback_query(lambda call: call.data == "change_wallet")
async def change_wallet(call: CallbackQuery, state: FSMContext):
    user_id = str(call.from_user.id)
    users = load_users()

    if user_id in users:
        current_wallet = users[user_id]['wallet']
        await call.message.answer(f"현재 등록된 지갑 주소는:\n{current_wallet}\n\n새로운 지갑 주소를 입력해주세요:")
        await state.set_state(ChangeWalletState.waiting_for_new_wallet)
    else:
        await call.message.answer("❗️ 가입되어 있지 않습니다. 먼저 가입해주세요.")
    await call.answer()

@dp.message(ChangeWalletState.waiting_for_new_wallet)
async def save_new_wallet(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    users = load_users()
    new_wallet = message.text

    if not new_wallet.startswith('0x') and len(new_wallet) < 20:
        await message.answer("❗️올바른 지갑 주소를 입력해주세요.")
        return

    users[user_id]['wallet'] = new_wallet
    save_users(users)

    await message.answer(f"✅ 지갑 주소가 변경되었습니다!\n새 주소: {new_wallet}")
    await state.clear()
    await show_menu(message, user_id)

@dp.callback_query(lambda call: call.data == "auto_meme")
async def auto_meme_info(call: CallbackQuery):
    message = (
        "💸 *1 SOL을 보내시면...*\n\n"
        "10개의 밈코인이 발행될 때마다\n"
        "0.9 SOL어치가 귀하의 지갑에 자동으로 입금됩니다.\n\n"
        "🚀 진정한 탈중앙 밈투자의 시작입니다.\n"
        "📍 (본 기능은 데모입니다. 실제 입금은 발생하지 않습니다)"
    )
    await call.message.answer(message, parse_mode="Markdown")
    await call.answer()


# === 실행 ===
async def main():
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
