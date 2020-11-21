# Amsterdam map data wrangling with Python/SQL

## Map Area
**Amsterdam, the Netherlands**

This work is to investigate the OpenStreeMap(osm) data in Amsterdam, the capitcal city of The Netherlands. 

Amsterdam is a city of fun. It is my favoriate city in the Netherlands, so I am curious about the map data about Amsterdam. The map data was downloaded from [Mapzen.com](https://mapzen.com/data/metro-extracts/your-extracts/29919fb164b6)

### Process:
The osm file was first parsed with python codes. ** Nodes, node tags, ways, way tags and way-node-tags** are extracted from the osm file and writen in **csv** files. The csv files were then imported into the **sqlite database**.

## Explore of the Map

Overall the data information looks quite clean, only a few problems are obvious:

- Phone numbers are spaced differently, some even has parenthesis. 
- Postcode shall be formatted with 4 digits + two alphabetic letters with a space.

For instance:

- Phone number:
```
<tag k="phone" v="+31 20 625 5537"/>
<tag k="phone" v=" 0900-8020"/>
<tag k="phone" v="+31 (0) 20 788 3060"/>
```

- postcode:
```
<tag k="addr:postcode" v="1071 ZD"/>
<tag k="addr:postcode" v="1016CJ"/>
```

## Code for correcting the phone.
The phone format in the Netherlands is 

- country code(+31) + digits 
- 9 digits in general while some public service code has only 7 or 8 digits
- for domestic calls, use 0 at the beginning instead of +31:
    for intance 0206278954 is the same as +31206278954

The phone number were formatted using the following functions.

```
def is_phone(elem): #find all the keys indicating phone number
    return (elem.attrib['k'] == 'phone')
  
def phone_format(elem):
    m = re.findall(r'([0-9]*)', t.attrib['v']) 
    #find all the digits only
    if m > -1:
        phone_f = ''.join(m)
        if len(phone_f) == 11 or len(phone_f) == 9: 
        #11 or digits means the digits are already correct
        # only need to omit all the parenthesis and space
            temp['value'] = '+'+phone_f
        elif len(phone_f) == 12:
        #12 digits means there's an extra 0 which shall be omited 
        # like this:  +31 (0)20 62 55 975
            temp['value'] = '+'+phone_f[:2] + phone_f[3:]
        elif len(phone_f) == 10 or len(phone_f) == 8:
        #10 or 8 digits means there's country code missing
        # like this:  0900-8020
            temp['value'] = '+31'+phone_f[1:]
        elif len(phone_f) == 13:
        #13 digits: 00 31 900 8020
            temp['value'] = '+'+phone_f[2:]
        elif len(phone_f) == 7:
            temp['value'] = '+31'+phone_f
        else:
            temp['value'] = phone_f
```

- Before: 0206278954
- After: +31206278954

- Before:  +31 (0)20 62 55 975
- After: +31206255975

- Before:  0900-8020
- After: +319008020


## Code for correcting postcode format.
The postcode format in the Netherlands is 

- 4 digits + 2 letters
- the official format has a space between digit and letter, however, some of the postcodes omit the space

The postcode values were formatted using the following functions.

```
def is_postcode(elem):
# to find out the postcodes
    return (elem.attrib['k'] == 'postcode')

def postcode_format(elem):
    el = elem.attrib['v']
    if len(el.strip()) != 7:
        return el.lstrip()[0:4]+" "+ el.rstrip()[-2:]
```

```
t.attrib['k'] == 'phone':
    if len(t.attrib['v']) == 12:
        temp['value'] = t.attrib['v']
    else:
        m = re.findall(r'([0-9]*)', t.attrib['v'])
        if m > -1:
            temp['value'] = '+'+''.join(m)
            print t.attrib['v']
            print temp['value']

if temp['key'] == 'postcode':
    temp['value']=t.attrib['v'].lstrip()[0:4]+" "+ t.attrib['v'].rstrip()[-2:]
    print t.attrib['v']
    print temp['value']
```

The results are as follows:
```
Before: 1074CM
After: 1074 CM

Before: 1073BP
After: 1073 BP
```

## Investigate the database

The size of the files:
```
for doc in [OSM_PATH, NODES_PATH , NODE_TAGS_PATH, WAYS_PATH, WAY_NODES_PATH, WAY_TAGS_PATH]:
    print doc, "...", os.path.getsize(doc)/1024/1024, "MB"
    
ams_s.osm ... 54 MB
nodes.csv ... 16 MB
nodes_tags.csv ... 11 MB
ways.csv ... 1 MB
ways_nodes.csv ... 4 MB
ways_tags.csv ... 3 MB
```

The statistics of the database:
```
sqlite> .dbinfo
...
database page size:  4096
database page count: 9042
schema format:       4
number of tables:    5
schema size:         763
...
```

Number of nodes:
```
sqlite> select count(*) from nodes;
197331
```
Number of ways:
```
sqlite> select count(*) from ways;
22391
```
Number of Unique users:
```
sqlite> select count(distinct(alles.uid))
   ...> from (select uid from nodes union all select uid from ways) alles;
519
```
## Coffee shop v.s. cafe
Whenever you want to find a cup of coffee in Amsterdam(or in the Netherlands), a quick note is that you can only find coffee in "cafe", while in "coffee shop" you may find something drives you in the vision. Yes, all the weed shop here is called "coffee shop". 

Here I am curious how many cafe has a name with "coffee", and how many of them are actually coffee shop rather than a normal cafe.
```
select * 
from node_tags 
where key = 'name' and value LIKE 'coffee%';
---

442122358,name,"Coffeeshop Basjoe",regular
524111724,name,"Coffeeshop Tops",regular
1221433734,name,"Coffee company",regular
1289051062,name,"Coffeeshop Paradox",regular
```

With the COUNT and LIKE methods, we can see that 13 of 22 places with a "coffee" are actually coffee shop for weed smoking.
So for those who doesn't know coffee shop, there's a 59% posibility that you will get wrong energy from "coffee" place in Amsterdam.

```
sqlite> select count(*) 
   ...> from node_tags 
   ...> where key = 'name' and value LIKE 'coffee%shop%';
13
sqlite> select count(*) 
   ...> from node_tags 
   ...> where key = 'name' and value LIKE 'coffee%';
22
```
## Postcodes - the most busiest area
Here comes the top postcodes which includes the most nodes. It is shown that 1018 DN has the most nodes (416)
```
sqlite> select value, count(distinct(node_tags.id)) as num 
   ...> from node_tags 
   ...> where node_tags.key = 'postcode'
   ...> group by node_tags.value
   ...> order by num desc
   ...> limit 10;
   
"1018 DN",416
"1015 DT",259
"1018 VG",238
"1011 HB",231
"1016 XP",162
"1011 PG",139
"1011 TD",135
"1011 DD",126
"1015 RR",122
"1017 AN",119
```


## Top 10 amenities
The total amenities amount is 1650,  37.33 % of the amenities are restaurants and fast food, while 15.2% of the amenities are pub and bars, at the top 6 it is bicycle parking, which again proves that Dutch people are bike maniac :)

```
sqlite> select value, count(*) as num
   ...> from node_tags
   ...> where key= 'amenity'
   ...> group by node_tags.value
   ...> order by num DESC
   ...> limit 10;
    
restaurant,486
pub,189
cafe,188
fast_food,130
bench,86
bar,62
bicycle_parking,55
post_box,48
atm,36
recycling,29
```


## Suggestions
According to the investigation, the map data is generally clean. However, it's noticed that the map data is not complete. The most busies aera(with most amenities) according to the postcode statistics is actually not in the city center, and when it comes to the coffee shop, there's only 13 on the list, which is even less then the amount in the famous red light district. 

For further imporvement of the data, it is suggested to give some format guide on the data input, for instance the location data. Some of the lat and lon value are not the same length, this might be due to different resolution of the GPS gadgets. Specifically, an initial check can be done to investigate if the resolution is the same as normal, and also if there are same value existed but with different node id, a question mark shall be intrigued once these events happen.

However, the format guide shall be in an appropriate way, too much details will also annoy the users, so a more friendly interface shall be proposed. 