import json
import os
from string import Template
current_dir = os.path.dirname(os.path.realpath(__file__)) 
html_template = """
<iframe id="chart" width="500" height="700" frameborder="0" seamless="seamless" scrolling="no" src="$scat_url.embed?width=500&height=700"></iframe>
<iframe id="chart" width="500" height="700" frameborder="0" seamless="seamless" scrolling="no" src="$hist_url.embed?width=500&height=700"></iframe>
<br>
"""
json_data=open(os.path.join(current_dir, 'urls.json'))
data = json.load(json_data)
json_data.close()
html = '<center>'
for url in data.values():
    try:
        html += Template(html_template).substitute({'hist_url': url['hist'], 'scat_url': url['scat']})
    except:
        pass
html += '</center>'
print html
