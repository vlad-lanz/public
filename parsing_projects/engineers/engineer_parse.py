from selenium.webdriver import Firefox
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep
import datetime as dt

browser = Firefox()
url = 'https://analytics.engineers2030.ru/schools'
browser.get(url)
sleep(10)

soup = BeautifulSoup(browser.page_source, 'lxml')

# в find_all список всех универов, из каждого достаю по href ссылке
all_urls = [elem.get('href') for elem in soup.find_all('a', {'class': 'schools-list__item'})]

final_df = pd.DataFrame()
for url in all_urls:
    university_url = f"https://analytics.engineers2030.ru{url}development-program/indicators"
    browser.get(university_url)
    sleep(10)  # чтобы страница прогрузилась

    if not browser.find_elements(By.TAG_NAME, 'button'):  # на случай, если страница не загрузится
        continue
    else:
        for button in browser.find_elements(By.TAG_NAME, 'button')[1:]:  # раскрываю каждую кнопку
            button.send_keys(Keys.ENTER)

        # всё раскрыто, наливаю суп
        soup = BeautifulSoup(browser.page_source, 'lxml')

        university_name = soup.find('span', {'class': 'school-head__school-name'}).text  # имя универа

        # это болванка, к которой справа будут присовокупляться все показатели
        university_df = pd.DataFrame({'Университет': [university_name for _ in range(9)],
                                      'Год': range(2022, 2031)})

        all_tables = soup.find_all('div', {'class': 'card__inner'})[1:]  # здесь все таблички

        for table in all_tables:
            index_name = table.find_all('div')[0].text.strip()  # имя показателя
            # по -1 индексу данные, убираю оттуда ненужные знаки
            full_data = table.find_all('div')[-1].text.replace('\xa0', '').replace(',', '.').split()

            years = [int(x) for x in full_data[2::2]]  # прохожу по нужным индексам, меняя тип данных
            index_data = [float(x) for x in full_data[3::2]]

            data_df = pd.DataFrame({'Год': years,  # создаю df на основе данных, который потом распличу
                                    'Данные': index_data})

            plan = data_df.iloc[:9].rename({'Данные': f'{index_name} (план)'}, axis=1)  # спличу на 2
            fact = data_df.iloc[9:].rename({'Данные': f'{index_name} (факт)'}, axis=1)
            combined = pd.merge(plan, fact, how='outer', on='Год').drop('Год', axis=1)  # и объединяю

            combined["% выполнения"] = (combined.iloc[:, 1] / combined.iloc[:, 0]) * 100
            combined["% выполнения"] = combined["% выполнения"].apply(lambda x: 100.0 if x >= 100 else x).round(2)
            combined.insert(0, "% выполнения", combined.pop("% выполнения"))

            # затем конкатенирую с болванкой, которая наполняется
            university_df = pd.concat([university_df, combined], axis=1)

        final_df = pd.concat([final_df, university_df], axis=0)  # присоединяю к общей таблице

final_df = final_df.sort_values(['Год', 'Университет'])

for_export = pd.DataFrame()
shapka = final_df.iloc[:, :2]
counter = 0

# прохожу по таблице с шагом в 3 и для каждой делаю ранг
while final_df.iloc[:, counter + 2:counter + 5].shape[1] != 0:
    current_index = pd.concat([shapka, final_df.iloc[:, counter + 2:counter + 5]], axis=1)  # склеиваю с шапкой

    # склеивал ради группировки и проставления рангов в группе
    current_index['Ранг'] = current_index.groupby('Год')['% выполнения'].rank(ascending=False, method='min')
    current_index = current_index.iloc[:, 2:]  # расклеиваю с шапкой
    current_index.insert(0, 'Ранг', current_index.pop('Ранг'))
    for_export = pd.concat([for_export, current_index], axis=1)
    counter += 3  # сдвиг на 3

for_export = pd.concat([shapka, for_export], axis=1)

for_export['№'] = range(1, for_export.shape[0] + 1)
for_export.insert(0, '№', for_export.pop('№'))

for_export = for_export.fillna('')

file_name = rf'C:/python_work/выгрузки/пиш/пиш_{dt.date.today()}.xlsx'
for_export.to_excel(file_name, index=False)
