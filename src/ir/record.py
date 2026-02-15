from uuid import uuid4

from typing import Optional, List
from dataclasses import dataclass, field


@dataclass
class Record:
    id: str = field(default_factory=lambda: "")

    # Personal Information
    firstName: Optional[str] = None
    middleName: Optional[str] = None
    lastName: Optional[str] = None
    
    # Demographics
    gender: Optional[str] = None
    ethnicity: Optional[str] = None
    dobYear: Optional[int] = None
    dobMonth: Optional[int] = None
    dobDay: Optional[int] = None
    
    party: Optional[str] = None
    
    # Location Information
    houseNumber: Optional[str] = None
    road: Optional[str] = None
    unit: Optional[str] = None
    pobox: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postcode: Optional[str] = None
    country: Optional[str] = None
    continent: Optional[str] = None
    location: Optional[str] = None
    latLong: Optional[str] = None
    
    # Vehicle Information
    autoMake: Optional[str] = None
    autoModel: Optional[str] = None
    autoYear: Optional[str] = None
    autoBody: Optional[str] = None
    autoClass: Optional[str] = None
    vin: Optional[str] = None
    VRN: Optional[str] = None
    
    # Contact Information
    emails: List[str] = field(default_factory=list)
    phoneNumbers: List[str] = field(default_factory=list)
    usernames: List[str] = field(default_factory=list)
    
    # Network Information
    ips: List[str] = field(default_factory=list)
    imei: Optional[str] = None
    domain: Optional[str] = None
    asn: Optional[int] = None
    asnOrg: Optional[str] = None
    accuracy_radius: Optional[int] = None
    links: List[str] = field(default_factory=list)

    # Browser Information
    userAgent: Optional[str] = None

    # User generated content
    message: Optional[str] = None
    
    # Financial Information
    income: Optional[str] = None
    
    # Security/Credentials
    passwords: List[str] = field(default_factory=list)
    
    # Metadata
    source: Optional[str] = None
    line: Optional[str] = None
    
    # Additional Fields
    notes: List[str] = field(default_factory=list)
    photos: List[str] = field(default_factory=list)

    # How new is the data
    recencyYear: Optional[int] = None
    recencyMonth: Optional[int] = None
    recencyDay: Optional[int] = None

    def __init__(self):
        self.id = None
        self.firstName = None
        self.middleName = None
        self.lastName = None
        self.gender = None
        self.ethnicity = None
        self.dob = None
        self.party = None
        self.address = None
        self.city = None
        self.state = None
        self.zipCode = None
        self.country = None
        self.continent = None
        self.location = None
        self.latLong = None
        self.autoMake = None
        self.autoModel = None
        self.autoYear = None
        self.autoBody = None
        self.autoClass = None
        self.vin = None
        self.VRN = None
        self.emails = []
        self.phoneNumbers = []
        self.usernames = []
        self.ips = []
        self.domain = None
        self.asn = None
        self.asnOrg = None
        self.accuracy_radius = None
        self.links = []
        self.income = None
        self.passwords = []
        self.source = None
        self.line = None
        self.userAgent = None
        self.message = None
        self.notes = []
        self.photos = []

    def to_dict(self):
        record = {
            k: (v.strip() if isinstance(v, str) else v) 
            for k, v in self.__dict__.items() 
            if v is not None and v != []
        }
        # Add ID if not already present
        if "id" not in record or record["id"] == "":
            record["id"] = str(uuid4())
        return record
    
    def add_or_set_value(self, key: str, value):
        if hasattr(self, key):
            current_value = getattr(self, key)
            
            # Handle List fields (append/extend)
            if isinstance(current_value, list):
                if isinstance(value, list):
                    current_value.extend(value)
                else:
                    current_value.append(value)
            
            # Handle String fields (concatenate with space if existing)
            elif isinstance(current_value, str) and current_value.strip():
                updated_str = f"{current_value} {value}"
                setattr(self, key, updated_str)
            
            # Handle None or empty values (overwrite)
            else:
                setattr(self, key, value)