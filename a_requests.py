import asyncio

import aiohttp
from more_itertools import chunked

from models import Session, SwapiPeople, init_orm

MAX_REQUEST = 10

async def get_people(person_id, http_session):

    response = await http_session.get(f"https://swapi.dev/api/people/{person_id}/")
    json_data = await response.json()

# данные по films
    films_urls = json_data.get('films', [])
    tasks_films = []
    for film_url in films_urls:
        task = asyncio.create_task(fetch_film_title(film_url, http_session))
        tasks_films.append(task)
    film_titles = await asyncio.gather(*tasks_films)

# данные по species
    species_urls = json_data.get('species', [])
    tasks_species = []
    for n_url in species_urls:
        task = asyncio.create_task(fetch_name(n_url, http_session))
        tasks_species.append(task)
    spec_titles = await asyncio.gather(*tasks_species)

# данные по starships
    starships_urls = json_data.get('starships', [])
    tasks_starships = []
    for n_url in starships_urls:
        task = asyncio.create_task(fetch_name(n_url, http_session))
        tasks_starships.append(task)
    starship_titles = await asyncio.gather(*tasks_starships)

# данные по vehicles
    vehicles_urls = json_data.get('vehicles', [])
    tasks_vehicles = []
    for n_url in vehicles_urls:
        task = asyncio.create_task(fetch_name(n_url, http_session))
        tasks_vehicles.append(task)
    vehicl_titles = await asyncio.gather(*tasks_vehicles)

    return {
        'birth_year': json_data.get('birth_year', ''),
        'eye_color': json_data.get('eye_color', ''),
        'films': ', '.join(film_titles),
        'gender': json_data.get('gender', ''),
        'hair_color': json_data.get('hair_color', ''),
        'height': json_data.get('height', ''),
        'homeworld': json_data.get('homeworld', ''),
        'mass': json_data.get('mass', ''),
        'name': json_data.get('name', ''),
        'skin_color': json_data.get('skin_color', ''),
        'species': ', '.join(spec_titles),
        'starships': ', '.join(starship_titles),
        'vehicles': ', '.join(vehicl_titles)
    }

# вспомогательные функции для обработки данных по URL
async def fetch_film_title(film_url, http_session):
    response = await http_session.get(film_url)
    film_data = await response.json()
    return film_data['title']

async def fetch_name(n_url, http_session):
    response = await http_session.get(n_url)
    film_data = await response.json()
    return film_data['name']

async def insert(jsons_list):
    async with Session() as db_session:
        orm_objects = [SwapiPeople(**json_item) for json_item in jsons_list]
        db_session.add_all(orm_objects)
        await db_session.commit()

async def main():
    await init_orm()
    async with aiohttp.ClientSession() as http_session:
        for people_id_chunk in chunked(range(1, 101), MAX_REQUEST):
            coros = [get_people(i, http_session) for i in people_id_chunk]
            jsons_list = await asyncio.gather(*coros)
            task = asyncio.create_task(insert(jsons_list))
    tasks_set = asyncio.all_tasks()
    current_task = asyncio.current_task()
    tasks_set.remove(current_task)
    await asyncio.gather(*tasks_set)

asyncio.run(main())
