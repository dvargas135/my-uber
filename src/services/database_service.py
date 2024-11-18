from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func
import threading

Base = declarative_base()

# Define ORM models corresponding to the database tables

# class Taxi(Base):
#     __tablename__ = 'taxis'
    
#     taxi_id = Column(Integer, primary_key=True)
#     pos_x = Column(Integer, nullable=False)
#     pos_y = Column(Integer, nullable=False)
#     speed = Column(Integer, nullable=False)
#     status = Column(String(20), nullable=False)
#     connected = Column(Boolean, default=True)
#     stopped = Column(Boolean, default=False)
#     last_updated = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    
#     # Relationships
#     assignments = relationship("Assignment", back_populates="taxi")
#     heartbeats = relationship("Heartbeat", back_populates="taxi")


class User(Base):
    __tablename__ = 'users'
    
    user_id = Column(Integer, primary_key=True)
    pos_x = Column(Integer, nullable=False)
    pos_y = Column(Integer, nullable=False)
    waiting_time = Column(Integer, nullable=False)
    request_time = Column(TIMESTAMP, server_default=func.now())
    
    # Relationships
    assignments = relationship("Assignment", back_populates="user")


class Assignment(Base):
    __tablename__ = 'assignments'
    
    assignment_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('users.user_id'), nullable=False)
    taxi_id = Column(Integer, ForeignKey('taxis.taxi_id'), nullable=False)
    assignment_time = Column(TIMESTAMP, server_default=func.now())
    status = Column(String(20), nullable=False)
    
    # Relationships
    user = relationship("User", back_populates="assignments")
    taxi = relationship("Taxi", back_populates="assignments")


class Heartbeat(Base):
    __tablename__ = 'heartbeat'
    
    heartbeat_id = Column(Integer, primary_key=True, autoincrement=True)
    taxi_id = Column(Integer, ForeignKey('taxis.taxi_id'), nullable=False)
    timestamp = Column(TIMESTAMP, server_default=func.now())
    
    # Relationships
    taxi = relationship("Taxi", back_populates="heartbeats")


class DatabaseService:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, db_url):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(DatabaseService, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_url):
        if self._initialized:
            return
        self.engine = create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self._initialized = True
        print("DatabaseService initialized and connected to the database.")
    
    def get_session(self):
        return self.Session()
    
    # CRUD Operations for Taxis
    def add_taxi(self, taxi_id, pos_x, pos_y, speed, status):
        session = self.get_session()
        try:
            taxi = Taxi(
                taxi_id=taxi_id,
                pos_x=pos_x,
                pos_y=pos_y,
                speed=speed,
                status=status
            )
            session.add(taxi)
            session.commit()
            print(f"Taxi {taxi_id} added to the database.")
        except Exception as e:
            session.rollback()
            print(f"Error adding taxi {taxi_id}: {e}")
        finally:
            session.close()
    
    def update_taxi_position(self, taxi_id, pos_x, pos_y):
        session = self.get_session()
        try:
            taxi = session.query(Taxi).filter(Taxi.taxi_id == taxi_id).first()
            if taxi:
                taxi.pos_x = pos_x
                taxi.pos_y = pos_y
                taxi.last_updated = func.now()
                session.commit()
                print(f"Taxi {taxi_id} position updated to ({pos_x}, {pos_y}).")
            else:
                print(f"Taxi {taxi_id} not found in the database.")
        except Exception as e:
            session.rollback()
            print(f"Error updating taxi {taxi_id} position: {e}")
        finally:
            session.close()
    
    def set_taxi_status(self, taxi_id, status):
        session = self.get_session()
        try:
            taxi = session.query(Taxi).filter(Taxi.taxi_id == taxi_id).first()
            if taxi:
                taxi.status = status
                taxi.last_updated = func.now()
                session.commit()
                print(f"Taxi {taxi_id} status set to {status}.")
            else:
                print(f"Taxi {taxi_id} not found in the database.")
        except Exception as e:
            session.rollback()
            print(f"Error setting taxi {taxi_id} status: {e}")
        finally:
            session.close()
    
    def mark_taxi_unavailable(self, taxi_id):
        self.set_taxi_status(taxi_id, "unavailable")
    
    def mark_taxi_available(self, taxi_id):
        self.set_taxi_status(taxi_id, "available")
    
    # CRUD Operations for Users
    def add_user_request(self, user_id, pos_x, pos_y, waiting_time):
        session = self.get_session()
        try:
            user = User(
                user_id=user_id,
                pos_x=pos_x,
                pos_y=pos_y,
                waiting_time=waiting_time
            )
            session.add(user)
            session.commit()
            print(f"User {user_id} request added to the database.")
        except Exception as e:
            session.rollback()
            print(f"Error adding user {user_id}: {e}")
        finally:
            session.close()
    
    # CRUD Operations for Assignments
    def assign_taxi_to_user(self, user_id, taxi_id):
        session = self.get_session()
        try:
            assignment = Assignment(
                user_id=user_id,
                taxi_id=taxi_id,
                status="assigned"
            )
            session.add(assignment)
            # Update taxi status
            taxi = session.query(Taxi).filter(Taxi.taxi_id == taxi_id).first()
            if taxi:
                taxi.status = "unavailable"
                taxi.connected = False
            session.commit()
            print(f"Taxi {taxi_id} assigned to User {user_id}.")
        except Exception as e:
            session.rollback()
            print(f"Error assigning Taxi {taxi_id} to User {user_id}: {e}")
        finally:
            session.close()
    
    # CRUD Operations for Heartbeats
    def record_heartbeat(self, taxi_id):
        session = self.get_session()
        try:
            heartbeat = Heartbeat(
                taxi_id=taxi_id
            )
            session.add(heartbeat)
            session.commit()
            print(f"Heartbeat recorded for Taxi {taxi_id}.")
        except Exception as e:
            session.rollback()
            print(f"Error recording heartbeat for Taxi {taxi_id}: {e}")
        finally:
            session.close()
    
    # Additional Methods
    def get_available_taxis(self):
        session = self.get_session()
        try:
            taxis = session.query(Taxi).filter(Taxi.status == "available", Taxi.connected == True).all()
            return taxis
        except Exception as e:
            print(f"Error fetching available taxis: {e}")
            return []
        finally:
            session.close()
    
    def get_taxi_by_id(self, taxi_id):
        session = self.get_session()
        try:
            taxi = session.query(Taxi).filter(Taxi.taxi_id == taxi_id).first()
            return taxi
        except Exception as e:
            print(f"Error fetching Taxi {taxi_id}: {e}")
            return None
        finally:
            session.close()
    
    def get_user_by_id(self, user_id):
        session = self.get_session()
        try:
            user = session.query(User).filter(User.user_id == user_id).first()
            return user
        except Exception as e:
            print(f"Error fetching User {user_id}: {e}")
            return None
        finally:
            session.close()
    
    def update_taxi_connected_status(self, taxi_id, connected):
        session = self.get_session()
        try:
            taxi = session.query(Taxi).filter(Taxi.taxi_id == taxi_id).first()
            if taxi:
                taxi.connected = connected
                session.commit()
                status = "connected" if connected else "disconnected"
                print(f"Taxi {taxi_id} marked as {status}.")
            else:
                print(f"Taxi {taxi_id} not found in the database.")
        except Exception as e:
            session.rollback()
            print(f"Error updating Taxi {taxi_id} connected status: {e}")
        finally:
            session.close()
