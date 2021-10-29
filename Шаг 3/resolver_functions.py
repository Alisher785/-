import random

is_filled_Gov = False
is_filled_Mos = False


def resolve(**kwargs):
    return kwargs['resolve_fun'](kwargs['vals'])

def get_longest_val(vals):
    val1, val2 = vals[0], vals[2]
    return val2 if len(val1) <= len(val2) else val1

def get_random_val(vals):
    val1, src1, val2, src2 = vals[0], vals[1], vals[2], vals[3]
    return val1 if random.randint(0, 1) == 1 else val2

def get_older_val(vals):
    val1, val2 = vals[0], vals[2] 
    if val1 == '01.01.1970 00:00:00' or val2 == '01.01.1970 00:00:00':
        return val1 if val1 != '01.01.1970 00:00:00' else val2
    val1, val2 = val1.split(), val2.split()
    time_1 = val1[1].split(':')[::-1] + val1[0].split('.')
    time_2, only_time_2 = val2[1].split(':')[::-1] + val2[0].split('.')
    for i in range(len(time_1) - 1, -1, -1):
        if int(time_1[i]) == int(time_2[i]):
            continue
        else: return vals[0] if time_1[i] > time_2[i] else vals[2]
    return vals[0]

def get_mean_val(vals):
    val1, val2 = vals[0], vals[2]
    return (val1 + val2) // 2

def get_dataMos_val(vals):
    val1, src1, val2, src2 = vals[0], vals[1], vals[2], vals[3]
    return val1 if src1 == 'dataMosutf.json' else val2

def get_dataGov_val(vals):
    val1, src1, val2, src2 = vals[0], vals[1], vals[2], vals[3]
    return val1 if src1 == 'dataGovutf.json' else val2

def get_not_null_val(vals):
    val1, val2 = vals[0], vals[2]
    return val1 if val2 is None else val2


resolver_funcs = {
    "duplicate_id": get_random_val,
    "global_id": get_dataMos_val,
    "Name": get_dataGov_val,
    "PayWay": get_dataMos_val,
    "signature_date": get_older_val,
    "Address": get_longest_val,
    "Latitude_WGS84": get_not_null_val,
    "ValidUniversalServicesCard": get_random_val,
    "IntercityConnectionPayment": get_random_val,
    "ID": get_mean_val
}
