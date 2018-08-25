# database connection strings to be modified

import time
import datetime
import ccxt

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from jinja2 import Environment

import MySQLdb as bitdb

# send email reports per day
def gather_data_for_email_report(cursor):
    sql = "SELECT COUNT(*), market FROM \
           oneminmarket \
           WHERE \
           time_of_entry > DATE_SUB(NOW(), INTERVAL 24 HOUR) GROUP BY market"
    cursor.execute(sql);

    data_points_per_market = {}

    for row in cursor.fetchall():
        data_points_per_market[row[1]] = [row[0], (row[0]/1440) * 100] 

    cursor.close()
    create_and_send_email_report(data_points_per_market)



def create_and_send_email_report(data_points_per_market):
    number_of_markets = len(data_points_per_market)

    # jinja template
    template = """ 
      <html>
        <head>
          <title>{{ title }}</title>
        </head>  
        <body>
          <h4>Total Number of Markets = {{ number_of_markets }}</h4>
          <h3> Quality of data for each market </h3>
          <table border="1">
            <tr>
              <th>Market Name</th>
              <th>Data Points Expected</th>
              <th>Data Points Available</th>
              <th>Points Fetched</th>
            </tr>
            {% for key in data_points_per_market %}
            <tr>
              <td>{{ key }}</td>
              <td>1440</td>
              <td>{{ data_points_per_market[key][0] }}</td>
              <td>{{ data_points_per_market[key][1] }}</td>
            </tr>
            {% endfor %}
          </table>
        </body>
      </html>
    """

    msg = MIMEText(
            Environment().from_string(template).render(title='Market Details',
              number_of_markets=number_of_markets,
              data_points_per_market=data_points_per_market
                ), "html"
            )

    msg['Subject'] = 'Market Report'
    msg['From'] = '********'
    msg['To'] = '*******'
    # msg['To'] = '*****myemail'
    msg.add_header('Content-Type', 'text/html')

    password = '#######'

    server = smtplib.SMTP('smtp.gmail.com: 587')
    server.starttls()
    server.login(msg['From'], password)

    server.sendmail(msg['From'], msg['To'], msg.as_string())
    server.quit()

    print "successfully sent email to %s:" % (msg['To'])
    

   
if __name__ == "__main__":
    db = bitdb.connect(
            host='mysqlbittrex.********.ap-south-1.rds.amazonaws.com',
            port=3306,
            user='*******',
            passwd='*********',
            db='bittrex_exchange'
        )
    cursor = db.cursor()
    gather_data_for_email_report(cursor)

    cursor.close()
    db.close()
