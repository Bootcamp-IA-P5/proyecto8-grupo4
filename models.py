#!/usr/bin/env python3
"""
Modelos de datos para parsear mensajes de Kafka del random_generator
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class PersonalData:
    """Datos personales"""
    name: str
    last_name: str
    sex: str
    telfnumber: str
    passport: str
    email: str

    @classmethod
    def from_dict(cls, data: dict) -> 'PersonalData':
        return cls(
            name=data.get('name', ''),
            last_name=data.get('last_name', ''),
            sex=data.get('sex'),
            telfnumber=data.get('telfnumber', ''),
            passport=data.get('passport', ''),
            email=data.get('email', '')
        )


@dataclass
class Location:
    """Datos de ubicación"""
    fullname: str
    city: str
    address: str

    @classmethod
    def from_dict(cls, data: dict) -> 'Location':
        return cls(
            fullname=data.get('fullname', ''),
            city=data.get('city', ''),
            address=data.get('address', '')
        )


@dataclass
class ProfessionalData:
    """Datos profesionales"""
    fullname: str
    company: str
    company_address: str
    company_telfnumber: str
    company_email: str
    job: str

    @classmethod
    def from_dict(cls, data: dict) -> 'ProfessionalData':
        return cls(
            fullname=data.get('fullname', ''),
            company=data.get('company', ''),
            company_address=data.get('company address', ''),
            company_telfnumber=data.get('company_telfnumber', ''),
            company_email=data.get('company_email', ''),
            job=data.get('job', '')
        )


@dataclass
class BankData:
    """Datos bancarios"""
    passport: str
    IBAN: str
    salary: str

    @classmethod
    def from_dict(cls, data: dict) -> 'BankData':
        return cls(
            passport=data.get('passport', ''),
            IBAN=data.get('IBAN', ''),
            salary=data.get('salary', '')
        )


@dataclass
class NetData:
    """Datos de red"""
    address: str
    IPv4: str

    @classmethod
    def from_dict(cls, data: dict) -> 'NetData':
        return cls(
            address=data.get('address', ''),
            IPv4=data.get('IPv4', '')
        )


def identify_message_type(data: dict) -> str:
    """
    Identifica el tipo de mensaje basándose en sus campos

    Args:
        data: Diccionario con los datos del mensaje

    Returns:
        str: Tipo de mensaje ('personal', 'location', 'professional', 'bank', 'net', 'unknown')
    """
    keys = set(data.keys())

    # Personal data: name, last_name, sex, telfnumber, passport, email
    if {'name', 'last_name', 'passport', 'email'}.issubset(keys):
        return 'personal'

    # Location: fullname, city, address (sin company)
    if {'fullname', 'city', 'address'}.issubset(keys) and 'company' not in keys:
        return 'location'

    # Professional data: fullname, company, job
    if {'fullname', 'company', 'job'}.issubset(keys):
        return 'professional'

    # Bank data: passport, IBAN, salary
    if {'passport', 'IBAN', 'salary'}.issubset(keys):
        return 'bank'

    # Net data: address, IPv4 (solo estos dos)
    if keys == {'address', 'IPv4'}:
        return 'net'

    return 'unknown'
