from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import socketio
import motor.motor_asyncio
from datetime import datetime
from bson import ObjectId
import os
import re
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="DRIP Cafe API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

sio = socketio.AsyncServer(async_mode='asgi', cors_allowed_origins='*')
socket_app = socketio.ASGIApp(sio, other_asgi_app=app)

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URL)
db = client.drip_cafe
FALLBACK_IMAGE_URL = "https://images.unsplash.com/photo-1495474472287-4d71bcdd2085?auto=format&fit=crop&w=900&q=80"

class OrderItem(BaseModel):
    product_id: str
    name: str
    quantity: int
    price: float
    category: str | None = None
    image: str | None = None

class OrderCreate(BaseModel):
    customer_name: str
    phone_number: str
    table_number: str
    items: list[OrderItem]
    subtotal: float
    tax: float
    discount: float
    total_amount: float
    coupon_code: str | None = None

class MenuItemCreate(BaseModel):
    name: str
    description: str | None = None
    price: float
    category: str
    stock: int
    image: str | None = None

class MenuItemUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    price: float | None = None
    category: str | None = None
    stock: int | None = None
    image: str | None = None

class CouponCreate(BaseModel):
    code: str
    discount_percent: float
    description: str | None = None
    active: bool = True

class CouponUpdate(BaseModel):
    code: str | None = None
    discount_percent: float | None = None
    description: str | None = None
    active: bool | None = None

def str_objectid(doc):
    if doc and "_id" in doc:
        doc["id"] = str(doc["_id"])
        del doc["_id"]
    return doc

def menu_lookup_query(product_id: str):
    queries = [{"product_id": product_id}]
    if ObjectId.is_valid(product_id):
        queries.append({"_id": ObjectId(product_id)})
    return {"$or": queries}

def normalize_menu_doc(item):
    item = str_objectid(item)
    item["id"] = item.get("product_id") or item.get("id")
    image = item.get("image") or item.get("image_url") or FALLBACK_IMAGE_URL
    if len(image) > 500 or "..." in image or "imgrefurl=" in image or not image.startswith(("http://", "https://", "/")):
        image = FALLBACK_IMAGE_URL
    item["image"] = image
    item["image_url"] = image
    return item

def normalize_coupon_doc(coupon):
    coupon = str_objectid(coupon)
    coupon["code"] = coupon.get("code", "").upper()
    coupon["discount_percent"] = float(coupon.get("discount_percent", 0))
    coupon["active"] = bool(coupon.get("active", True))
    return coupon

def slugify(value: str):
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "menu-item"

async def unique_product_id(name: str):
    base = f"custom-{slugify(name)}"
    product_id = base
    suffix = 2
    while await db.menu.find_one({"product_id": product_id}):
        product_id = f"{base}-{suffix}"
        suffix += 1
    return product_id

async def queue_ahead_for_order(order):
    if not order or order.get("status") in {"Ready", "Served"}:
        return 0

    timestamp = order.get("timestamp")
    if not timestamp:
        return 0

    return await db.orders.count_documents({
        "timestamp": {"$lt": timestamp},
        "status": {"$in": ["Pending", "Preparing"]},
    })

@app.on_event("startup")
async def startup_db_client():
    # Insert starter menu if empty. Use stable product_id values so frontend orders
    # can be matched to MongoDB inventory without requiring ObjectId ids.
    menu_count = await db.menu.count_documents({})
    if menu_count == 0:
        dummy_menu = [
            {"product_id": "coffee-espresso", "name": "Signature Espresso", "description": "A concentrated, glossy shot with a caramel crema.", "price": 100, "category": "Coffee", "stock": 50, "image": "https://images.unsplash.com/photo-1514432324607-a09d9b4aefdd?auto=format&fit=crop&w=900&q=80"},
            {"product_id": "coffee-latte", "name": "Blue Velvet Latte", "description": "Smooth espresso folded into steamed milk with a soft finish.", "price": 150, "category": "Coffee", "stock": 30, "image": "https://images.unsplash.com/photo-1461023058943-07fcbe16d735?auto=format&fit=crop&w=900&q=80"},
            {"product_id": "coffee-mocha", "name": "Midnight Mocha", "description": "Chocolate, espresso, and milk with a deep cafe-style body.", "price": 190, "category": "Signature Drinks", "stock": 25, "image": "https://images.unsplash.com/photo-1578314675249-a6910f80cc4e?auto=format&fit=crop&w=900&q=80"},
            {"product_id": "iced-cold-brew", "name": "Slow Drip Cold Brew", "description": "Low-acid cold coffee brewed slowly for a clean finish.", "price": 160, "category": "Iced Tea", "stock": 24, "image": "https://images.unsplash.com/photo-1460931674309-0b2c70d2a9cb?auto=format&fit=crop&w=900&q=80"},
            {"product_id": "iced-peach", "name": "Peach Iced Tea", "description": "Bright tea with peach syrup, lemon, and crushed ice.", "price": 130, "category": "Iced Tea", "stock": 18, "image": "https://images.unsplash.com/photo-1556679343-c7306c1976bc?auto=format&fit=crop&w=900&q=80"},
            {"product_id": "dessert-tiramisu", "name": "Classic Tiramisu", "description": "Coffee-soaked layers with mascarpone and cocoa.", "price": 240, "category": "Desserts", "stock": 9, "image": "https://images.unsplash.com/photo-1571877227200-a0d98ea607e9?auto=format&fit=crop&w=900&q=80"},
            {"product_id": "snack-croissant", "name": "Butter Croissant", "description": "Flaky laminated pastry served warm.", "price": 120, "category": "Snacks", "stock": 22, "image": "https://images.unsplash.com/photo-1555507036-ab1f4038808a?auto=format&fit=crop&w=900&q=80"},
            {"product_id": "dessert-brownie", "name": "Fudge Brownie", "description": "Dense chocolate brownie with a glossy top.", "price": 140, "category": "Desserts", "stock": 14, "image": "https://images.unsplash.com/photo-1606313564200-e75d5e30476c?auto=format&fit=crop&w=900&q=80"},
        ]
        await db.menu.insert_many(dummy_menu)

    coupon_count = await db.coupons.count_documents({})
    if coupon_count == 0:
        await db.coupons.insert_many([
            {"code": "DRIP10", "discount_percent": 10, "description": "10% off", "active": True},
            {"code": "STUDENT15", "discount_percent": 15, "description": "15% off", "active": True},
        ])

@app.get("/api/menu")
async def get_menu():
    cursor = db.menu.find({})
    menu = await cursor.to_list(length=100)
    return [normalize_menu_doc(item) for item in menu]

@app.post("/api/admin/menu")
async def create_menu_item(item: MenuItemCreate):
    if item.price < 0:
        raise HTTPException(status_code=400, detail="Price cannot be negative")
    if item.stock < 0:
        raise HTTPException(status_code=400, detail="Stock cannot be negative")

    menu_item = item.dict()
    menu_item["product_id"] = await unique_product_id(item.name)
    image = item.image or FALLBACK_IMAGE_URL
    if len(image) > 500 or "..." in image or "imgrefurl=" in image or not image.startswith(("http://", "https://", "/")):
        image = FALLBACK_IMAGE_URL
    menu_item["image"] = image
    menu_item["image_url"] = image

    result = await db.menu.insert_one(menu_item)
    created = await db.menu.find_one({"_id": result.inserted_id})
    await sio.emit("inventory_update", {})
    return normalize_menu_doc(created)

@app.patch("/api/admin/menu/{item_id}")
async def update_menu_item(item_id: str, item: MenuItemUpdate):
    update = {key: value for key, value in item.dict(exclude_unset=True).items() if value is not None}
    if "price" in update and update["price"] < 0:
        raise HTTPException(status_code=400, detail="Price cannot be negative")
    if "stock" in update and update["stock"] < 0:
        raise HTTPException(status_code=400, detail="Stock cannot be negative")
    if "image" in update:
        image = update["image"] or FALLBACK_IMAGE_URL
        if len(image) > 500 or "..." in image or "imgrefurl=" in image or not image.startswith(("http://", "https://", "/")):
            image = FALLBACK_IMAGE_URL
        update["image"] = image
        update["image_url"] = image
    if not update:
        raise HTTPException(status_code=400, detail="No fields to update")

    result = await db.menu.update_one(menu_lookup_query(item_id), {"$set": update})
    if result.matched_count != 1:
        raise HTTPException(status_code=404, detail="Menu item not found")

    updated = await db.menu.find_one(menu_lookup_query(item_id))
    await sio.emit("inventory_update", {})
    return normalize_menu_doc(updated)

@app.delete("/api/admin/menu/{item_id}")
async def delete_menu_item(item_id: str):
    result = await db.menu.delete_one(menu_lookup_query(item_id))
    if result.deleted_count != 1:
        raise HTTPException(status_code=404, detail="Menu item not found")

    await sio.emit("inventory_update", {})
    return {"status": "success"}

@app.get("/api/coupons")
async def get_active_coupons():
    cursor = db.coupons.find({"active": True}).sort("code", 1)
    coupons = await cursor.to_list(length=100)
    return [normalize_coupon_doc(coupon) for coupon in coupons]

@app.get("/api/coupons/validate/{code}")
async def validate_coupon(code: str):
    coupon = await db.coupons.find_one({"code": code.strip().upper(), "active": True})
    if not coupon:
        raise HTTPException(status_code=404, detail="Coupon not found or inactive")
    return normalize_coupon_doc(coupon)

@app.get("/api/admin/coupons")
async def get_all_coupons():
    cursor = db.coupons.find({}).sort("code", 1)
    coupons = await cursor.to_list(length=100)
    return [normalize_coupon_doc(coupon) for coupon in coupons]

@app.post("/api/admin/coupons")
async def create_coupon(coupon: CouponCreate):
    code = coupon.code.strip().upper()
    if not code:
        raise HTTPException(status_code=400, detail="Coupon code is required")
    if coupon.discount_percent <= 0 or coupon.discount_percent > 100:
        raise HTTPException(status_code=400, detail="Discount must be between 1 and 100")
    if await db.coupons.find_one({"code": code}):
        raise HTTPException(status_code=400, detail="Coupon code already exists")

    coupon_dict = coupon.dict()
    coupon_dict["code"] = code
    result = await db.coupons.insert_one(coupon_dict)
    created = await db.coupons.find_one({"_id": result.inserted_id})
    await sio.emit("coupon_update", {})
    return normalize_coupon_doc(created)

@app.patch("/api/admin/coupons/{coupon_id}")
async def update_coupon(coupon_id: str, coupon: CouponUpdate):
    update = {key: value for key, value in coupon.dict(exclude_unset=True).items() if value is not None}
    if "code" in update:
        update["code"] = update["code"].strip().upper()
        if not update["code"]:
            raise HTTPException(status_code=400, detail="Coupon code is required")
        duplicate = await db.coupons.find_one({"code": update["code"], "_id": {"$ne": ObjectId(coupon_id)}})
        if duplicate:
            raise HTTPException(status_code=400, detail="Coupon code already exists")
    if "discount_percent" in update and (update["discount_percent"] <= 0 or update["discount_percent"] > 100):
        raise HTTPException(status_code=400, detail="Discount must be between 1 and 100")
    if not update:
        raise HTTPException(status_code=400, detail="No fields to update")

    result = await db.coupons.update_one({"_id": ObjectId(coupon_id)}, {"$set": update})
    if result.matched_count != 1:
        raise HTTPException(status_code=404, detail="Coupon not found")

    updated = await db.coupons.find_one({"_id": ObjectId(coupon_id)})
    await sio.emit("coupon_update", {})
    return normalize_coupon_doc(updated)

@app.delete("/api/admin/coupons/{coupon_id}")
async def delete_coupon(coupon_id: str):
    result = await db.coupons.delete_one({"_id": ObjectId(coupon_id)})
    if result.deleted_count != 1:
        raise HTTPException(status_code=404, detail="Coupon not found")

    await sio.emit("coupon_update", {})
    return {"status": "success"}

@app.post("/api/orders")
async def create_order(order: OrderCreate):
    # Generate token
    token = f"D-{datetime.now().strftime('%H%M')}-{ObjectId().binary.hex()[-4:].upper()}"

    order_items = []
    for item in order.items:
        if item.quantity <= 0:
            raise HTTPException(status_code=400, detail=f"Invalid quantity for {item.name}")

        menu_item = await db.menu.find_one(menu_lookup_query(item.product_id))
        if menu_item and menu_item.get("stock", 0) < item.quantity:
            raise HTTPException(status_code=400, detail=f"{item.name} has only {menu_item.get('stock', 0)} left")

        order_items.append({
            "product_id": item.product_id,
            "name": item.name,
            "quantity": item.quantity,
            "price": item.price,
            "line_total": item.price * item.quantity,
            "category": item.category,
            "image": item.image,
        })

    order_dict = order.dict()
    order_dict["items"] = order_items
    order_dict["token_number"] = token
    order_dict["status"] = "Pending"
    order_dict["timestamp"] = datetime.now()

    if order.coupon_code:
        coupon = await db.coupons.find_one({"code": order.coupon_code.strip().upper(), "active": True})
        if not coupon:
            raise HTTPException(status_code=400, detail="Coupon not found or inactive")
        discount = order.subtotal * (float(coupon.get("discount_percent", 0)) / 100)
        tax = (order.subtotal - discount) * 0.05
        order_dict["coupon_code"] = coupon["code"]
        order_dict["discount"] = discount
        order_dict["tax"] = tax
        order_dict["total_amount"] = order.subtotal - discount + tax
    
    # Stock deduction, only when the item is present in MongoDB inventory.
    for item in order.items:
        await db.menu.update_one(menu_lookup_query(item.product_id), {"$inc": {"stock": -item.quantity}})
    
    result = await db.orders.insert_one(order_dict)
    order_dict["id"] = str(result.inserted_id)
    order_dict.pop("_id", None)
    
    # Notify admin
    await sio.emit("new_order", order_dict)
    # Notify inventory update
    await sio.emit("inventory_update", {})
    
    return order_dict

@app.get("/api/orders/track/{token}")
async def track_order(token: str):
    order = await db.orders.find_one({"token_number": token})
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    order["queue_ahead"] = await queue_ahead_for_order(order)
    return str_objectid(order)

@app.get("/api/admin/orders")
async def get_all_orders():
    cursor = db.orders.find({}).sort("timestamp", -1)
    orders = await cursor.to_list(length=100)
    return [str_objectid(o) for o in orders]

@app.patch("/api/admin/orders/{order_id}/status")
async def update_order_status(order_id: str, status: str):
    result = await db.orders.update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"status": status}}
    )
    if result.modified_count == 1:
        order = await db.orders.find_one({"_id": ObjectId(order_id)})
        await sio.emit("order_status_update", {"id": order_id, "status": status, "token_number": order["token_number"]})
        return {"status": "success"}
    raise HTTPException(status_code=400, detail="Update failed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:socket_app", host="0.0.0.0", port=8000, reload=True)
