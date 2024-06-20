import requests
import time
import pandas as pd
import datetime as dt

base_api_url = 'https://api.socio.center/public/priority'
answer = requests.get(f"{base_api_url}/list")
answer_json = answer.json()

# здесь получаю данные по id и shortname универов
if answer.status_code == 200 and answer_json['status'] == 'success':
    university_names = pd.DataFrame(answer_json['data']['participants'])
    # карта применяется для перевода 1-5 в нужные значения в столбике
    university_names.group = university_names.group.map(answer_json['data']['group'])
    university_names = university_names.loc[university_names.level == '1']  # трек "Исследовательское лидерство"
else:
    raise Exception(f"{answer.status_code} не 200, либо {answer_json['status']} не success")

assert university_names.shape[0] == 17, "Число университетов не равно 17!"

combined_universities = pd.DataFrame()  # здесь суммарные данные

for index, row in university_names.iterrows():  # перебираю универы
    university_id, university_name = row['id'], row['shortName']
    university_url = f"{base_api_url}/{university_id}/indicators"
    university_answer = requests.get(university_url)
    university_json = university_answer.json()

    if university_answer.status_code == 200 and university_json['status'] == 'success':
        # здесь данные с базового и специального треков
        indicators_data = university_json['data'][0]['elements'] + university_json['data'][1]['elements']
        all_indicators_df = pd.DataFrame()  # это суммарная таблица по универу

        for indicator in indicators_data:
            indicator_name = indicator['indicator']  # имя индикатора
            current_indicator = pd.DataFrame()  # здесь будет лежать суммарная информация по каждому из показателей

            # в data лежат данные по самому показателю: факт и план, а в calculationData расчётные показатели
            for sub_indicator in indicator['data'] + indicator['calculationData']:
                # внутри лежит словарь с 10 связками ключ-значение, где напротив каждого года - его значение
                all_data = sub_indicator['data'].items()

                # здесь данные по каждому столбику по отдельности
                # делаю формат таким, чтобы на месте индекса был год, а единственный столбик - данные
                current_sub_indicator = pd.DataFrame(all_data).set_index(0).rename(
                    {1: f"{sub_indicator['description']}"}, axis=1)

                current_indicator = pd.concat([current_indicator, current_sub_indicator], axis=1).fillna(0)

            current_indicator = current_indicator.rename(
                {"Отражение факта (по итогам года)": f"{indicator_name} факт",
                 'План': f"{indicator_name} план"}, axis=1)

            all_indicators_df = pd.concat([all_indicators_df, current_indicator], axis=1)
    else:
        raise Exception(f'''По университету {university_name}
        {university_answer.status_code} не 200, либо {university_json['status']} не success''')

    all_indicators_df['Университет'] = university_name
    combined_universities = pd.concat([combined_universities, all_indicators_df])
    time.sleep(0.5)

combined_universities['Год'] = combined_universities.index
combined_universities = combined_universities.astype({'Год': 'int64'}).sort_values(['Год', 'Университет'])
combined_universities['№'] = range(1, combined_universities.shape[0] + 1)

# удаляю ненужные столбики по базовому треку
combined_universities = pd.concat([combined_universities.iloc[:, :6], combined_universities.iloc[:, 51:]], axis=1)

for column_name in ['Год', 'Университет', '№']:
    combined_universities.insert(0, column_name, combined_universities.pop(column_name))

file_name = rf'C:/python_work/выгрузки/программа_2030/специальная_часть_{dt.date.today()}.xlsx'
combined_universities.to_excel(file_name, sheet_name='Показатели', index=False)
