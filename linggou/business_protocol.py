"""Shared strict downlink whitelist for the PC/Foxglove route (ASCII lines)."""
import re

def allowed_interaction(cmd):
    if not isinstance(cmd,str):
        return False
    if cmd in {"$q","$q-","$w","$w-","$v","$v-","$c","$c-","$hi","$cart"}:
        return True
    if re.fullmatch(r"\$i(?:0[1-9]|1[01])",cmd):
        return True
    price=re.fullmatch(r"\$PRICE,(0[1-9]|1[01]),([0-9]{1,5})",cmd)
    if price:
        return 1<=int(price.group(2))<=99999
    weight=re.fullmatch(r"\$SIM,WEIGHT,([0-9]{1,5})",cmd)
    return bool(weight and 1<=int(weight.group(1))<=50000)
