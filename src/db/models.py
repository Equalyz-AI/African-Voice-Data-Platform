from sqlmodel import Field, SQLModel, Column, Relationship, CheckConstraint
from typing import List, Optional
from datetime import datetime, timezone
import sqlalchemy.dialects.postgresql  as pg
from sqlalchemy.types import TIMESTAMP
from sqlalchemy.sql import func
import uuid
from enum import Enum
from sqlalchemy import JSON


class RoleEnum(str, Enum):
    user = "user"
    admin = "admin"

class Categroy(str, Enum):
    read = "read"
    spontaneous = "spontaneous"
    read_as_spontaneous = "read_as_spontaneous"

class GenderEnum(str, Enum):
    male = "male"
    female = "female"


class User(SQLModel, table=True):
    __tablename__ = "users"

    id: str = Field(
        sa_column=Column(
            pg.VARCHAR,
            nullable=False,
            primary_key=True,
            default= lambda: str(uuid.uuid4())
        )
    )
    full_name: str = Field(nullable=True)
    email: str = Field(unique=True, nullable=False)
    password: str = Field(nullable=False)
    is_verified: bool = Field(default=False)
    verification_token: Optional[str] = Field(default=None)
    created_at: datetime = Field(sa_column=Column(pg.TIMESTAMP, default=datetime.now))
    updated_at: datetime = Field(sa_column=Column(pg.TIMESTAMP, default=datetime.now, onupdate=datetime.utcnow))

    feedback: List["Feedback"] = Relationship(back_populates="user")


class AudioSample(SQLModel, table=True):
    __table_args__ = (
        CheckConstraint("snr >= 0", name="check_snr_non_negative"),
        CheckConstraint("gender IN ('male','female')", name="check_valid_gender"),
    )
    id: str = Field(
        sa_column=Column(
            pg.VARCHAR,
            nullable=False,
            primary_key=True,
            default=lambda: str(uuid.uuid4())
        )
    )

    dataset_id: Optional[str] = Field(foreign_key="dataset.id", nullable=True, default="naija")
    annotator_id: str
    sentence_id: str
    sentence: str
    storage_link: str
    gender: Optional[str] =  Field(sa_column=Column(pg.VARCHAR, default=None))

    age_group: Optional[str] =  Field(sa_column=Column(pg.VARCHAR, default=None, nullable=True))
    edu_level: Optional[str] =  Field(sa_column=Column(pg.VARCHAR, default=None, nullable=True))
    durations: Optional[str] =  Field(sa_column=Column(pg.VARCHAR, default=None, nullable=True))

    language: Optional[str] =  Field(sa_column=Column(pg.VARCHAR, default='naija'))
    snr:  Optional[int] = Field(sa_column=Column(pg.INTEGER, default=40))

    domain: Optional[str] =  Field(sa_column=Column(pg.VARCHAR, default=None))
    category: str = Field(sa_column=Column(pg.VARCHAR, default=Categroy.read))
    created_at: datetime = Field(sa_column=Column(pg.TIMESTAMP, default=datetime.now))
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)



class AudioTag(SQLModel, table=True):
    id: str = Field(
        sa_column=Column(
            pg.VARCHAR,
            nullable=False,
            primary_key=True,
            default= lambda: str(uuid.uuid4())
        )
    )
    audio_id: str = Field(foreign_key="audiosample.id")
    key: str
    value: str


class QAMetadata(SQLModel, table=True):
    id: str = Field(
        sa_column=Column(
            pg.VARCHAR,
            nullable=False,
            primary_key=True,
            default= lambda: str(uuid.uuid4())
        )
    )
    audio_id: str = Field(foreign_key="audiosample.id", unique=True)
    qa_status: str  # 'passed', 'failed', 'pending'
    duration_check: bool
    noise_level: str
    label_match: bool
    confidence_score: float
    reviewed_by: str  # 'auto' or 'human'
    reviewed_at: datetime = Field(default_factory=datetime.utcnow)


class Dataset(SQLModel, table=True):
    id: str = Field(
        sa_column=Column(
            pg.VARCHAR,
            nullable=False,
            primary_key=True,
            default= lambda: str(uuid.uuid4())
        )
    )
    name: str
    description: Optional[str] = None
    created_by: str = Field(foreign_key="users.id")
    created_at: datetime = Field(default_factory=datetime.utcnow)


class DownloadLog(SQLModel, table=True):
    id: str = Field(
        sa_column=Column(
            pg.VARCHAR,
            nullable=False,
            primary_key=True,
            default= lambda: str(uuid.uuid4())
        )
    )
    user_id: str = Field(foreign_key="users.id")
    dataset_id: str = Field(foreign_key="dataset.id")
    percentage: int  # from {5,20,40,60,80,100}
    timestamp: datetime = Field(default_factory=datetime.utcnow)


# User feedback. The feedback should be a list

class Feedback(SQLModel, table=True):
    __tablename__ = "feedback"

    id: str = Field(
        sa_column=Column(
            pg.VARCHAR,
            nullable=False,
            primary_key=True,
            default=lambda: str(uuid.uuid4())
        )
    )

    user_id: Optional[str] = Field(foreign_key="users.id", nullable=True, default=None)
    fullname: Optional[str] = Field(nullable=True, default=None)
    email: Optional[str] = Field(nullable=True, default=None)

    # Star rating (1–5)
    rating: Optional[int] = Field(default=None)

    # List of selected issues
    issues: Optional[List[str]] = Field(
        sa_column=Column(JSON, default=list)
    )

    # Other issue (free text)
    other_issue: Optional[str] = Field(default=None)

    # Suggestions for improvement
    suggestions: Optional[str] = Field(default=None)

    created_at: datetime = Field(sa_column=Column(pg.TIMESTAMP, default=datetime.now))

    user: Optional["User"] = Relationship(back_populates="feedback")

    def __repr__(self):
        return f"Feedback(user_id={self.user_id}, rating={self.rating}, issues={self.issues})"
