from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import clickhouse_connect
from datetime import datetime
import uuid

app = FastAPI()

# Подключение к ClickHouse
client = clickhouse_connect.get_client(host='127.0.0.1', port=8123, user='default', password='')

# Создание базы данных, если она не существует
client.command("CREATE DATABASE IF NOT EXISTS my_db")

# Список таблиц
tables = [
    "activeOrderWidgetPress", "add_cart_cart", "add_cart_favorite", "add_cart_item_page",
    "add_like_from_item_card", "add_like_from_item_page", "add_to_cart", "add_to_favorites",
    "app_clear_data", "app_remove", "app_update", "branch_screen_open", "change_subcategory",
    "checkout_click", "click_Cashbackmodal_delivery", "click_CashbackSilver_delivery",
    "click_presentPayment_delivery", "click_productReplacement_delivery",
    "click_storeHeader_favorite_screen", "del_cart_cart", "del_cart_favorite",
    "del_cart_item_page", "del_like_fr_favorite", "del_like_fr_item_page", "delivery_cancel_order",
    "activeOrderWidgetPress", "add_cart_cart", "add_cart_favorite", "add_cart_item_page",
    "add_like_from_item_card", "add_like_from_item_page",  "add_to_cart",
    "add_to_favorites", "app_clear_data", "app_remove", "app_update",
    "branch_screen_open", "change_subcategory", "checkout_click", "click_Cashbackmodal_delivery",
    "click_CashbackSilver_delivery", "click_presentPayment_delivery",
    "click_productReplacement_delivery", "click_storeHeader_favorite_screen",
    "del_cart_cart", "del_cart_favorite", "del_cart_item_page",
    "del_like_fr_favorite", "del_like_fr_item_page", "delivery_cancel_order",
    "delivery_swipe_cancel", "empty_trash_cart_screen", "empty_trash_cart_screen54",
    "first_open", "modal_push_notif_open", "nav_addressList_feed", "nav_addressList_profile",
    "nav_addressList_start", "nav_allCategory_feed", "nav_boonWallet_profile",
    "nav_categoryCart_category", "nav_categoryProduct_itemPage", "nav_feedSubsidiary",
    "nav_feedSubsidiary_address", "nav_ImageModal_itemPage", "nav_OrderCheckList_ordDelivery",
    "nav_OrderHistory_payMethods", "nav_OrderHistory_profile", "nav_Search_feed",
    "nav_Search_start", "nav_ShopList_another", "nav_ShopList_food_delivery",
    "nav_ShopList_tech", "nav_startScreen_cart", "nav_subCategory_feed642",
    "nav_subCategory_feed720", "nav_subCategory_feed721", "nav_subCategory_feed749",
    "nav_subCategory_feed773", "nav_subCategory_feed776", "nav_subCategory_feed778",
    "nav_subCategoryList_PL_feed", "navigate_EditProfile_profile_screen", "navigation_ShopList_in_start_page",
    "notification_dismiss", "notification_foreground", "notification_open", "notification_receive",
    "offer_newShopFeed", "offer_newShopShop_List", "offer_newShopStart", "open_aboutService_profile",
    "open_boonCoinHead_start", "open_categories_shoplist_another", "open_categories_shoplist_food_delivery",
    "open_categories_shoplist_food_market", "open_categories_shoplist_zoo_delivery", "open_category_cart_in_category_page",
    "open_category_in_feed", "open_commentModal_start", "open_contact_profile", "open_feed_product_in_start_page",
    "open_item_modal", "open_popular_section_in_feed_page", "open_search", "open_search_subCat",
    "open_search_subCatList", "open_StartTab_favorite_screen", "open_store_feed", "open_strs_AboutDelivery",
    "open_strs_ExpressDelivery", "open_strs_Loyalty", "open_strs_Registration", "open_strs_WhereMyOrder", "open_subCategory",
    "open_subcategory", "opn_modal_infoCard__feed", "opn_modal_infoCard__feed_BE", "os_update", "payment_type_pay_3",
    "press_bannerText_in_card_delivery_screen", "press_search_input_feed_page", "press_search_input_search_tab",
    "press_store_card", "press_tab", "press_telModal_feed", "press_telModal_feed_BE", "press_whatsModal_feed",
    "press_whatsModal_feed_BE", "promo_clear", "promo_submit", "screen_view", "search", "select_dayDelivery_screen",
    "session_start", "store_card_shopList", "swipe_payment_method", "switch_modal_item",
]

# Создание таблиц внутри базы my_db
for table in tables:
    client.command(f'''
        CREATE TABLE IF NOT EXISTS my_db.{table} (
            id UUID DEFAULT generateUUIDv4(),
            event_name String,
            count Int32,
            date DateTime DEFAULT now(),
            user_count Int32
        ) Engine = MergeTree 
        ORDER BY date
    ''')

# Модель данных для записи
class Record(BaseModel):
    event_name: str
    count: int
    user_count: int

# Добавление записи в таблицу
@app.post("/add/{table_name}")
def add_record(table_name: str, record: Record):
    if table_name not in tables:
        raise HTTPException(status_code=400, detail="Invalid table name")

    try:
        client.insert(
            f"my_db.{table_name}",
            [(str(uuid.uuid4()), record.event_name, record.count, datetime.now(), record.user_count)],
            column_names=['id', 'event_name', 'count', 'date', 'user_count']
        )
        return {"message": f"Record added to my_db.{table_name} successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Получение всех записей из таблицы
@app.get("/records/{table_name}")
def get_records(table_name: str):
    if table_name not in tables:
        raise HTTPException(status_code=400, detail="Invalid table name")

    try:
        records = client.query(f'SELECT * FROM my_db.{table_name}').result_rows
        return {"records": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Удаление записи по ID
@app.delete("/delete/{table_name}/{record_id}")
def delete_record(table_name: str, record_id: str):
    if table_name not in tables:
        raise HTTPException(status_code=400, detail="Invalid table name")

    try:
        client.command(f"ALTER TABLE my_db.{table_name} DELETE WHERE id = '{record_id}'")
        return {"message": f"Record {record_id} deleted from my_db.{table_name} successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
