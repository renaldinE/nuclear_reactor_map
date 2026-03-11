from os import listdir
from os.path import join, isfile
import numpy as np
import pandas as pd
import re
import json

# Directory of GeoJSON data downloaded from MapHub
data_dir = r'C:\Users\renald_e\Desktop\data_2025'

# List of file names in directory
fns = [f for f in listdir(data_dir) if f.endswith('.geojson')]

# Headers of data in the description 
data_headers = [
    'reactor_number',
    'nameplate_capacity', # (MW)
    'ave_annual_gen', # (GWh)
    'ave_capacity_factor', # (%)
    'co2_emission_save' # (cas worth/year)
    ]

rows = []
for fn in fns:
    with open(join(data_dir,fn), 'r', encoding="utf-8") as f:
        data = json.load(f)
        
    features = data['features']
    
    for entry in features:
        
        new_dict = {}
        new_dict['name'] = entry['properties']['title']
        new_dict['lat'] = entry['geometry']['coordinates'][0]
        new_dict['lon'] = entry['geometry']['coordinates'][1]
        
        if entry['properties'].get('marker-color'):
            new_dict['marker_col'] = entry['properties']['marker-color']
        else:
            new_dict['marker_col'] = np.nan
            
        if entry['properties'].get('marker_id'):
            new_dict['marker_id'] = entry['properties']['marker_id']
        else:
            new_dict['marker_id'] = np.nan
            
        country = fn.split('.')[0].split(' ')[0]
        new_dict['country'] = country
        
        if entry['properties'].get('description'):
            for line, header in zip(entry['properties']['description'].splitlines(), data_headers):
                line = line.strip()
                if not line or "http" in line:
                    continue
            
                match = re.match(r"^(.*?):\s*(.+)$", line)
                if match:
                    try:
                        value = float(match.group(2).replace(" ", "").strip())
                    except ValueError as err:
                        print(err)
                        print(country)
                        print(entry['properties']['title'])
                        
                        if 'no data' in match.group(2).lower():
                            value = np.nan
                        else:
                            try:
                                value = match.group(2).replace(" ", "").strip()
                                value = float(value.replace('~','').replace(r'%',''))
                            except:
                                value = np.nan
                    
                    new_dict[header] = value
        else:
            for header in data_headers:
                new_dict[header] = np.nan
        
        if 'operational' in fn.lower():
            new_dict['status'] = 'operational'
        else:
            new_dict['status'] = 'shutdown'
    
        rows.append(new_dict)
        
database = pd.DataFrame(rows)

fp = join(data_dir,'database.xlsx')
database.to_excel(fp, index=False)