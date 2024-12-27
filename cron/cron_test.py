from datetime import datetime
# test cron working
with open('cron_test.txt', 'a+', encoding='utf-8') as file:
    file.write(datetime.today().strftime('%Y-%m-%d-%H-%M-%S') + '\n')
