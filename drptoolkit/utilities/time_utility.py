import datetime

def get_iso_time_str() -> str:
   dt = datetime.datetime.now(tz=datetime.UTC) 

   return dt.isoformat(
       sep = 'T', 
       timespec = 'auto',
   )
    