from fastapi import FastAPI, APIRouter, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
import sqlite3
import json
import socket
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict, EmailStr
from typing import List, Optional, Any
import uuid
from datetime import datetime, timezone, timedelta
import bcrypt
import jwt
import asyncio
import secrets
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

mongo_url = os.environ.get('MONGO_URL', 'mongodb://localhost:27017')
db_name = os.environ.get('DB_NAME', 'swiftlogistics_db')


def _mongo_available() -> bool:
    try:
        with socket.create_connection(("localhost", 27017), timeout=1):
            return True
    except OSError:
        return False


def _sqlite_json_default(value):
    if isinstance(value, (datetime,)): return value.isoformat()
    if isinstance(value, uuid.UUID): return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


class SQLiteCursor:
    def __init__(self, conn: sqlite3.Connection, collection: str, query: Optional[dict] = None, projection: Optional[dict] = None):
        self.conn = conn
        self.collection = collection
        self.query = query or {}
        self.projection = projection or {}
        self.sort_field = None
        self.sort_direction = 1

    def sort(self, field: str, direction: int = -1):
        self.sort_field = field
        self.sort_direction = direction
        return self

    def _matches(self, document: dict) -> bool:
        for key, expected in self.query.items():
            if "$in" in expected if isinstance(expected, dict) else False:
                if document.get(key) not in expected["$in"]:
                    return False
                continue
            if document.get(key) != expected:
                return False
        return True

    def _project(self, document: dict) -> dict:
        if not self.projection:
            return document

        projected = {}
        include_all = True
        for key, value in self.projection.items():
            if key == "_id" and value == 0:
                continue
            if value == 0:
                include_all = False

        for key, value in document.items():
            if key == "_id":
                continue
            if value is None:
                continue

            if key in self.projection:
                if self.projection[key] == 0:
                    continue
                projected[key] = value
                continue

            if include_all:
                projected[key] = value

        return projected

    async def to_list(self, limit: Optional[int] = None):
        rows = self.conn.execute(
            "SELECT payload FROM collection_data WHERE collection = ? ORDER BY id ASC",
            (self.collection,)
        ).fetchall()
        documents = []
        for row in rows:
            document = json.loads(row[0])
            if self._matches(document):
                documents.append(self._project(document))

        if self.sort_field is not None:
            documents.sort(key=lambda item: item.get(self.sort_field, "") or "", reverse=self.sort_direction < 0)
        if limit is not None:
            documents = documents[:limit]
        return documents


class SQLiteCollection:
    def __init__(self, conn: sqlite3.Connection, name: str):
        self.conn = conn
        self.name = name

    async def find_one(self, query: Optional[dict] = None, projection: Optional[dict] = None):
        query = query or {}
        rows = self.conn.execute(
            "SELECT payload, id FROM collection_data WHERE collection = ? ORDER BY id ASC",
            (self.name,)
        ).fetchall()
        for payload, row_id in rows:
            document = json.loads(payload)
            matches = True
            for key, expected in query.items():
                if isinstance(expected, dict) and "$in" in expected:
                    if document.get(key) not in expected["$in"]:
                        matches = False
                        break
                elif document.get(key) != expected:
                    matches = False
                    break
            if matches:
                if not projection:
                    return document

                projected = {}
                include_all = True
                for key, value in projection.items():
                    if key == "_id" and value == 0:
                        continue
                    if value == 0:
                        include_all = False

                for key, value in document.items():
                    if key == "_id":
                        continue
                    if value is None:
                        continue
                    if key in projection:
                        if projection[key] == 0:
                            continue
                        projected[key] = value
                        continue
                    if include_all:
                        projected[key] = value
                return projected
        return None

    def find(self, query: Optional[dict] = None, projection: Optional[dict] = None):
        return SQLiteCursor(self.conn, self.name, query or {}, projection or {})

    async def insert_one(self, document: dict):
        payload = json.dumps(document, default=_sqlite_json_default)
        self.conn.execute(
            "INSERT INTO collection_data (collection, payload) VALUES (?, ?)",
            (self.name, payload)
        )
        self.conn.commit()
        return type("Result", (), {"inserted_id": 1})()

    async def update_one(self, query: dict, update: dict):
        document = await self.find_one(query)
        if document is None:
            return type("Result", (), {"matched_count": 0, "modified_count": 0})()

        if "$set" in update:
            document.update(update["$set"])
        if "$unset" in update:
            for key in update["$unset"]:
                document.pop(key, None)

        row = self.conn.execute(
            "SELECT id, payload FROM collection_data WHERE collection = ? ORDER BY id ASC",
            (self.name,)
        ).fetchall()
        for row_id, payload in row:
            if json.loads(payload) == await self.find_one(query):
                self.conn.execute(
                    "UPDATE collection_data SET payload = ? WHERE id = ?",
                    (json.dumps(document, default=_sqlite_json_default), row_id)
                )
                self.conn.commit()
                return type("Result", (), {"matched_count": 1, "modified_count": 1})()

        self.conn.execute(
            "INSERT INTO collection_data (collection, payload) VALUES (?, ?)",
            (self.name, json.dumps(document, default=_sqlite_json_default))
        )
        self.conn.commit()
        return type("Result", (), {"matched_count": 1, "modified_count": 1})()


class SQLiteDatabase:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def __getattr__(self, name: str):
        return SQLiteCollection(self.conn, name)


sqlite_conn = sqlite3.connect(ROOT_DIR / 'app.db', check_same_thread=False)
sqlite_conn.execute("""
    CREATE TABLE IF NOT EXISTS collection_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        collection TEXT NOT NULL,
        payload TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
""")
sqlite_conn.commit()

if os.environ.get('USE_SQLITE', 'true').lower() == 'true' or not _mongo_available():
    db = SQLiteDatabase(sqlite_conn)
else:
    client = AsyncIOMotorClient(mongo_url, serverSelectionTimeoutMS=2000)
    db = client[db_name]

DEMO_ACCOUNTS = {
    'admin@demo.com': {'password': 'admin123', 'role': 'admin', 'full_name': 'Demo Admin'},
    'customer@demo.com': {'password': 'customer123', 'role': 'customer', 'full_name': 'Demo Customer'},
}


async def seed_demo_accounts():
    for email, user_data in DEMO_ACCOUNTS.items():
        existing = await db.users.find_one({"email": email}, {"_id": 0})
        if existing:
            continue

        user = User(
            full_name=user_data['full_name'],
            gender='other',
            email=email,
            phone_number='0000000000',
            role=user_data['role']
        )
        doc = user.model_dump()
        doc['password_hash'] = hash_password(user_data['password'])
        doc['created_at'] = doc['created_at'].isoformat()
        await db.users.insert_one(doc)

# Gmail setup
GMAIL_USER = os.getenv('GMAIL_USER', '')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD', '').replace(' ', '')

# JWT setup
JWT_SECRET = os.getenv('JWT_SECRET')
if not JWT_SECRET:
    if os.getenv('ENVIRONMENT') == 'production':
        raise RuntimeError('JWT_SECRET must be configured in production.')
    JWT_SECRET = secrets.token_urlsafe(32)
JWT_ALGORITHM = 'HS256'
JWT_EXPIRATION_HOURS = 24

# Security
security = HTTPBearer()

app = FastAPI()
api_router = APIRouter(prefix="/api")

@app.on_event("startup")
async def startup_event():
    await seed_demo_accounts()

# ===== MODELS =====

class UserRegister(BaseModel):
    full_name: str
    gender: str
    email: EmailStr
    phone_number: str
    password: str
    role: str = "customer"

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class User(BaseModel):
    model_config = ConfigDict(extra="ignore")
    user_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    full_name: str
    gender: str
    email: EmailStr
    phone_number: str
    role: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class ShipmentBooking(BaseModel):
    logistic_type: str
    service_type: str
    from_location: str
    to_location: str
    receiver_name: str
    phone_number: str
    alternate_phone: str
    full_address: str
    pincode: str
    weight_kg: float
    volume_weight: float
    booking_date: str
    booking_time: str

class Shipment(BaseModel):
    model_config = ConfigDict(extra="ignore")
    shipment_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str
    tracking_id: str
    user_id: str
    user_email: str
    logistic_type: str
    service_type: str
    from_location: str
    to_location: str
    receiver_name: str
    phone_number: str
    alternate_phone: str
    full_address: str
    pincode: str
    weight_kg: float
    volume_weight: float
    booking_date: str
    booking_time: str
    estimated_price: Optional[float] = None
    otp: Optional[str] = None
    payment_status: str = "pending"
    order_status: str = "Order Submitted"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class AdminEmailRequest(BaseModel):
    order_id: str
    estimated_price: float

class OrderVerification(BaseModel):
    order_id: str
    otp: str

class PaymentRequest(BaseModel):
    order_id: str
    payment_method: str
    amount: float

class Payment(BaseModel):
    model_config = ConfigDict(extra="ignore")
    payment_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str
    amount: float
    payment_method: str
    payment_status: str = "completed"
    payment_date: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class PackagingSelection(BaseModel):
    order_id: str
    packing_type: str
    pickup_date: Optional[str] = None
    pickup_time: Optional[str] = None
    branch_location: Optional[str] = None

class PackagingDetails(BaseModel):
    model_config = ConfigDict(extra="ignore")
    packaging_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str
    packing_type: str
    pickup_date: Optional[str] = None
    pickup_time: Optional[str] = None
    branch_location: Optional[str] = None
    status: str = "scheduled"

class TrackingUpdate(BaseModel):
    model_config = ConfigDict(extra="ignore")
    update_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    shipment_id: str
    location: str
    status: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class DriverVehicleAssignment(BaseModel):
    shipment_id: str
    driver_name: str
    driver_license_number: str
    driver_phone: str
    vehicle_plate_number: str
    vehicle_type: str
    vehicle_capacity: str

class DriverVehicle(BaseModel):
    model_config = ConfigDict(extra="ignore")
    assignment_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    shipment_id: str
    driver_name: str
    driver_license_number: str
    driver_phone: str
    vehicle_plate_number: str
    vehicle_type: str
    vehicle_capacity: str

class DeliveryProofUpload(BaseModel):
    shipment_id: str
    delivery_photo: str
    parcel_condition_photo: str

class DeliveryProof(BaseModel):
    model_config = ConfigDict(extra="ignore")
    proof_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    shipment_id: str
    delivery_photo: str
    parcel_condition_photo: str
    delivery_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class FeedbackSubmission(BaseModel):
    shipment_id: str
    rating: int
    package_condition: str
    review_comment: str
    feedback_photo: Optional[str] = None

class Feedback(BaseModel):
    model_config = ConfigDict(extra="ignore")
    feedback_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    shipment_id: str
    rating: int
    package_condition: str
    review_comment: str
    feedback_photo: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class ShipmentStatusUpdate(BaseModel):
    order_id: str
    order_status: str

# ===== HELPER FUNCTIONS =====

def generate_high_entropy_id(prefix: str, byte_count: int = 8) -> str:
    random_suffix = secrets.token_hex(byte_count)
    return f"{prefix}{random_suffix}"

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))

def create_access_token(user_id: str, email: str, role: str) -> str:
    payload = {
        "user_id": user_id,
        "email": email,
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRATION_HOURS)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    token = credentials.credentials
    return decode_token(token)

async def get_admin_user(current_user: dict = Depends(get_current_user)) -> dict:
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user

def send_gmail_email(to_email: str, subject: str, html_content: str):
    """Send email using Gmail SMTP - works with ANY email address"""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        raise RuntimeError("Gmail is not configured. Set GMAIL_USER and GMAIL_APP_PASSWORD in backend/.env.")

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = GMAIL_USER
    msg['To'] = to_email
    msg.attach(MIMEText(html_content, 'html'))
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, to_email, msg.as_string())

# ===== AUTHENTICATION ROUTES =====

@api_router.post("/auth/register")
async def register(user_data: UserRegister):
    existing_user = await db.users.find_one({"email": user_data.email}, {"_id": 0})
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    user = User(
        full_name=user_data.full_name,
        gender=user_data.gender,
        email=user_data.email,
        phone_number=user_data.phone_number,
        role=user_data.role
    )
    doc = user.model_dump()
    doc['password_hash'] = hash_password(user_data.password)
    doc['created_at'] = doc['created_at'].isoformat()
    await db.users.insert_one(doc)
    token = create_access_token(user.user_id, user.email, user.role)
    return {"message": "User registered successfully", "token": token, "user": user.model_dump()}

@api_router.post("/auth/login")
async def login(credentials: UserLogin):
    user_doc = await db.users.find_one({"email": credentials.email}, {"_id": 0})
    if not user_doc:
        raise HTTPException(status_code=401, detail="Invalid email or password")
    if not verify_password(credentials.password, user_doc['password_hash']):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    token = create_access_token(user_doc['user_id'], user_doc['email'], user_doc['role'])
    user_doc.pop('password_hash', None)
    return {"message": "Login successful", "token": token, "user": user_doc}

@api_router.get("/auth/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    user_doc = await db.users.find_one({"user_id": current_user["user_id"]}, {"_id": 0, "password_hash": 0})
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")
    return user_doc

# ===== SHIPMENT BOOKING ROUTES =====

@api_router.post("/shipment/book")
async def book_shipment(booking_data: ShipmentBooking, current_user: dict = Depends(get_current_user)):
    order_id = generate_high_entropy_id("ORD")
    tracking_id = generate_high_entropy_id("TRK")
    shipment = Shipment(
        order_id=order_id,
        tracking_id=tracking_id,
        user_id=current_user["user_id"],
        user_email=current_user["email"],
        **booking_data.model_dump()
    )
    doc = shipment.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await db.shipments.insert_one(doc)
    return {"message": "Shipment booking submitted successfully", "order_id": order_id, "tracking_id": tracking_id, "shipment": shipment.model_dump()}

@api_router.get("/shipment/my-shipments")
async def get_my_shipments(current_user: dict = Depends(get_current_user)):
    shipments = await db.shipments.find({"user_id": current_user["user_id"]}, {"_id": 0}).to_list(1000)
    return {"shipments": shipments}

# ===== ADMIN ROUTES =====

@api_router.get("/admin/orders")
async def get_all_orders(current_user: dict = Depends(get_admin_user)):
    orders = await db.shipments.find({}).sort("created_at", -1).to_list(1000)
    for order in orders:
        order.pop("_id", None)
    return {"orders": orders}

@api_router.post("/admin/send-shipment-email")
async def send_shipment_email(request: AdminEmailRequest, current_user: dict = Depends(get_admin_user)):
    shipment = await db.shipments.find_one({"order_id": request.order_id}, {"_id": 0})
    if not shipment:
        raise HTTPException(status_code=404, detail="Order not found")

    # Generate random 6-digit OTP
    otp = str(secrets.randbelow(900000) + 100000)

    await db.shipments.update_one(
        {"order_id": request.order_id},
        {"$set": {"otp": otp, "estimated_price": request.estimated_price}}
    )

    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
        <div style="max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px;">
            <h2 style="color: #2563eb;">Shipment Cost Confirmation</h2>
            <p>Dear Customer,</p>
            <p>Thank you for choosing SwiftLogistics. Your shipment has been reviewed by our team.</p>
            <div style="background-color: #f0f9ff; padding: 15px; border-radius: 5px; margin: 20px 0;">
                <p style="margin: 5px 0;"><strong>Order ID:</strong> {request.order_id}</p>
                <p style="margin: 5px 0;"><strong>Estimated Shipping Cost:</strong> ₹{request.estimated_price}</p>
                <p style="margin: 10px 0;"><strong>Your OTP:</strong></p>
                <p style="font-size: 32px; font-weight: bold; color: #2563eb; letter-spacing: 8px; margin: 5px 0;">{otp}</p>
            </div>
            <p>Please login to SwiftLogistics and go to the <strong>Verify & Pay</strong> tab.</p>
            <p>Enter your <strong>Order ID</strong> and the <strong>OTP</strong> above to confirm and pay.</p>
            <p style="margin-top: 30px;">Best Regards,<br><strong>SwiftLogistics Team</strong></p>
        </div>
    </body>
    </html>
    """

    customer_email = shipment["user_email"]

    try:
        await asyncio.to_thread(send_gmail_email, customer_email, "Your Shipment Cost & OTP - SwiftLogistics", html_content)
        return {
            "status": "success",
            "message": f"Email sent successfully to {customer_email}",
            "otp": otp
        }
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}")
        # Return OTP even if email fails so admin can share manually
        return {
            "status": "email_failed",
            "message": f"Email failed. Share these details with customer manually.",
            "customer_email": customer_email,
            "otp": otp,
            "order_id": request.order_id,
            "estimated_price": request.estimated_price,
            "error": str(e)
        }

@api_router.put("/admin/update-shipment-status")
async def update_shipment_status(update: ShipmentStatusUpdate, current_user: dict = Depends(get_admin_user)):
    result = await db.shipments.update_one(
        {"order_id": update.order_id},
        {"$set": {"order_status": update.order_status}}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Order not found")
    return {"message": "Order status updated successfully"}

@api_router.post("/admin/assign-driver-vehicle")
async def assign_driver_vehicle(assignment: DriverVehicleAssignment, current_user: dict = Depends(get_admin_user)):
    driver_vehicle = DriverVehicle(**assignment.model_dump())
    doc = driver_vehicle.model_dump()
    await db.driver_vehicle.insert_one(doc)
    return {"message": "Driver and vehicle assigned successfully", "assignment": driver_vehicle.model_dump()}

@api_router.post("/admin/add-tracking-update")
async def add_tracking_update(update: TrackingUpdate, current_user: dict = Depends(get_admin_user)):
    doc = update.model_dump()
    doc['updated_at'] = doc['updated_at'].isoformat()
    await db.tracking_updates.insert_one(doc)
    return {"message": "Tracking update added successfully", "update": update.model_dump()}

@api_router.post("/admin/upload-delivery-proof")
async def upload_delivery_proof(proof: DeliveryProofUpload, current_user: dict = Depends(get_admin_user)):
    delivery_proof = DeliveryProof(**proof.model_dump())
    doc = delivery_proof.model_dump()
    doc['delivery_timestamp'] = doc['delivery_timestamp'].isoformat()
    await db.delivery_proof.insert_one(doc)
    await db.shipments.update_one(
        {"shipment_id": proof.shipment_id},
        {"$set": {"order_status": "Delivered"}}
    )
    return {"message": "Delivery proof uploaded successfully", "proof": delivery_proof.model_dump()}

@api_router.get("/admin/feedback-reports")
async def get_feedback_reports(current_user: dict = Depends(get_admin_user)):
    feedback = await db.feedback.find({}, {"_id": 0}).to_list(1000)
    return {"feedback": feedback}

# ===== ORDER VERIFICATION & PAYMENT ROUTES =====

@api_router.post("/order/verify")
async def verify_order(verification: OrderVerification, current_user: dict = Depends(get_current_user)):
    shipment = await db.shipments.find_one({"order_id": verification.order_id}, {"_id": 0})
    if not shipment:
        raise HTTPException(status_code=404, detail="Order not found")
    if shipment.get("otp") != verification.otp:
        raise HTTPException(status_code=400, detail="Invalid OTP")
    return {"message": "Order verified successfully", "estimated_price": shipment.get("estimated_price"), "order_details": shipment}

@api_router.post("/payment/process")
async def process_payment(payment_data: PaymentRequest, current_user: dict = Depends(get_current_user)):
    shipment = await db.shipments.find_one({"order_id": payment_data.order_id}, {"_id": 0})
    if not shipment:
        raise HTTPException(status_code=404, detail="Order not found")
    payment = Payment(
        order_id=payment_data.order_id,
        amount=payment_data.amount,
        payment_method=payment_data.payment_method
    )
    doc = payment.model_dump()
    doc['payment_date'] = doc['payment_date'].isoformat()
    await db.payments.insert_one(doc)
    await db.shipments.update_one(
        {"order_id": payment_data.order_id},
        {"$set": {"payment_status": "paid", "order_status": "Payment Completed"}}
    )
    return {"message": "Payment processed successfully", "payment": payment.model_dump()}

# ===== PACKAGING ROUTES =====

@api_router.post("/packaging/select")
async def select_packaging(packaging_data: PackagingSelection, current_user: dict = Depends(get_current_user)):
    packaging = PackagingDetails(**packaging_data.model_dump())
    doc = packaging.model_dump()
    await db.packaging_details.insert_one(doc)
    await db.shipments.update_one(
        {"order_id": packaging_data.order_id},
        {"$set": {"order_status": "Packaging Scheduled"}}
    )
    return {"message": "Packaging option selected successfully", "packaging": packaging.model_dump()}

# ===== TRACKING ROUTES =====

@api_router.get("/tracking/{tracking_id}")
async def track_shipment(tracking_id: str):
    shipment = await db.shipments.find_one({"tracking_id": tracking_id}, {"_id": 0})
    if not shipment:
        raise HTTPException(status_code=404, detail="Tracking ID not found")
    updates = await db.tracking_updates.find({"shipment_id": shipment["shipment_id"]}, {"_id": 0}).to_list(100)
    return {
        "tracking_id": tracking_id,
        "order_id": shipment["order_id"],
        "status": shipment["order_status"],
        "from_location": shipment["from_location"],
        "to_location": shipment["to_location"],
        "tracking_updates": updates
    }

# ===== FEEDBACK ROUTES =====

@api_router.post("/feedback/submit")
async def submit_feedback(feedback_data: FeedbackSubmission, current_user: dict = Depends(get_current_user)):
    feedback = Feedback(**feedback_data.model_dump())
    doc = feedback.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await db.feedback.insert_one(doc)
    await db.shipments.update_one(
        {"shipment_id": feedback_data.shipment_id},
        {"$set": {"order_status": "Order Completed"}}
    )
    return {"message": "Feedback submitted successfully", "feedback": feedback.model_dump()}

@api_router.get("/user/feedback-history")
async def get_feedback_history(current_user: dict = Depends(get_current_user)):
    user_shipments = await db.shipments.find({"user_id": current_user["user_id"]}, {"_id": 0}).to_list(1000)
    shipment_ids = [s["shipment_id"] for s in user_shipments]
    feedback = await db.feedback.find({"shipment_id": {"$in": shipment_ids}}, {"_id": 0}).to_list(1000)
    return {"feedback": feedback}

app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@app.on_event("shutdown")
async def shutdown_db_client():
    mongo_client = globals().get("client")
    if mongo_client is not None:
        mongo_client.close()