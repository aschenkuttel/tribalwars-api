from utils import Database, initiate_errors, parse_result
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, UJSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from slowapi import Limiter, _rate_limit_exceeded_handler  # noqa
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from typing import List, Union, Dict
from contextlib import asynccontextmanager
import utils
import uvicorn
import json

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="TW Connect API",
    description="Parsing the official tribal wars endpoints into an open and easy API.",
    version="0.2",
    swagger_ui_parameters={'defaultModelsExpandDepth': -1},
    redoc_url=None
)


def custom_openapi():
    if not app.openapi_schema:
        app.openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            openapi_version=app.openapi_version,
            description=app.description,
            terms_of_service=app.terms_of_service,
            contact=app.contact,
            license_info=app.license_info,
            routes=app.routes,
            tags=app.openapi_tags,
            servers=app.servers,
        )
        for _, method_item in app.openapi_schema.get('paths').items():
            for _, param in method_item.items():
                responses = param.get('responses')
                # remove 422 response, also can remove other status code
                if '422' in responses:
                    del responses['422']
    return app.openapi_schema


app.openapi = custom_openapi
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = Database()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    initiate_errors(_app)
    await db.connect()

    # waits till the end of the lifespan
    yield

    await db.disconnect()

@app.get('/', include_in_schema=False)
async def home():
    return RedirectResponse('/docs')


# WORLD
@app.get('/world',
         tags=["World"],
         response_model=Dict[str, List],
         summary="current supported worlds")
@limiter.limit('30/minute')
async def get_worlds(_: Request):
    response = await db.fetch('SELECT * FROM world ORDER BY world', with_world=True)
    result = {}

    for row in response:
        language, world = row['world'][:2], row['world']
        worlds = result.get(language)

        if worlds is None:
            result[language] = [world]
        else:
            worlds.append(world)

    return result


@app.get('/world/{language}',
         tags=["World"],
         response_model=List,
         summary="current supported worlds by language")
@limiter.limit('30/minute')
async def get_worlds_by_language(_: Request, language):
    if language not in db.languages:
        raise utils.error.InvalidArgument('language', language)

    response = await db.fetch('SELECT * FROM world WHERE world LIKE $1', language + "%", with_world=True)
    return [e['world'] for e in response]


@app.get('/world/setting/{world}',
         tags=["World"],
         summary="settings of given world")
async def get_world_settings(_: Request, world):
    if world not in db.worlds:
        raise utils.error.InvalidArgument('world', world)

    response = await db.fetchone('SELECT * FROM world WHERE world = $1', world, with_world=True)
    result = json.loads(response['config'])

    for key, value in result.items():
        for child_key, child_value in value.items():
            if not isinstance(child_value, str):
                continue

            if child_value.isdigit():
                value[child_key] = int(child_value)

            try:
                # converting decimals and negative numbers
                value[child_key] = float(child_value)
            except ValueError:
                pass

    return result


# VILLAGE
@app.get('/village/{world}',
         tags=["Village"],
         response_class=UJSONResponse,
         summary="villages of given world")
@limiter.limit('1/minute')
async def get_villages_by_world(_: Request, world):
    """# returns id -> village dictionary"""
    query = db.create_query('village', 'SELECT * FROM {}', world)
    response = await db.fetch(query, key='id')
    return parse_result(response, 'name', iterable=True)


@app.get('/village/{world}/by-tribe/{tribe_id}',
         tags=["Village"],
         response_model=List[utils.Village],
         summary="villages of given world and tribe id")
@limiter.limit('20/minute')
async def get_villages_by_tribe(_: Request, world, tribe_id: int):
    base_query = 'SELECT * FROM {} WHERE player_id IN (SELECT id FROM {} WHERE tribe_id = $1)'
    query = db.create_query(('village', 'player'), base_query, world)
    response = await db.fetch(query, tribe_id)
    tmp_result = parse_result(response, 'name', iterable=True)

    result = {}
    for entry in tmp_result:
        villages = result.get(entry['player_id'])
        if villages is None:
            result[entry['player_id']] = [entry]
        else:
            villages.append(entry)

    return result


@app.get('/village/{world}/by-player/{player_id}',
         tags=["Village"],
         response_model=List[utils.Village],
         summary="villages of given world and player id")
@limiter.limit('30/minute')
async def get_villages_by_player(_: Request, world, player_id: int):
    query = db.create_query('village', 'SELECT * FROM {} WHERE player_id = $1', world)
    response = await db.fetch(query, player_id)
    return parse_result(response, 'name', iterable=True)


@app.get('/village/{world}/by-id/{village_id}',
         tags=["Village"],
         response_model=utils.Village,
         summary="village of given world and village id")
@limiter.limit('30/minute')
async def get_village_by_id(_: Request, world, village_id: int):
    query = db.create_query('village', 'SELECT * FROM {} WHERE id = $1', world)
    response = await db.fetchone(query, village_id)
    return parse_result(response, 'name')


# PLAYER
@app.get('/player/{world}',
         tags=["Player"],
         response_class=UJSONResponse,
         summary="players of given world")
@limiter.limit('1/minute')
async def get_players_by_world(_: Request, world):
    query = db.create_query('player', 'SELECT * FROM {}', world)
    response = await db.fetch(query, key='id')
    return parse_result(response, 'name', iterable=True)


@app.get('/player/{world}/by-tribe/{tribe_id}',
         tags=["Player"],
         response_model=List[utils.Player],
         summary="players of given world and given tribe id")
@limiter.limit('30/minute')
async def get_players_by_tribe(_: Request, world, tribe_id: int):
    query = db.create_query('player', 'SELECT * FROM {} WHERE tribe_id = $1', world)
    response = await db.fetch(query, tribe_id, key='id')
    return parse_result(response, 'name', iterable=True)


@app.get('/player/{world}/by-name/{player_name}',
         tags=["Player"],
         response_model=utils.Player,
         summary="player of given world and player name")
@limiter.limit('30/minute')
async def get_player_by_name(_: Request, world, player_name):
    query = db.create_query('player', 'SELECT * FROM {} WHERE LOWER(name) = $1', world)
    response = await db.fetch(query, player_name.lower())
    return parse_result(response, 'name')


@app.get('/player/{world}/by-id/{player_id}',
         tags=["Player"],
         response_model=utils.Player,
         summary="player of given world and player id")
@limiter.limit('30/minute')
async def get_player_by_id(_: Request, world, player_id: int):
    query = db.create_query('player', 'SELECT * FROM {} WHERE id = $1', world)
    response = await db.fetchone(query, player_id)
    return parse_result(response, 'name')


# TRIBE
@app.get('/tribe/{world}',
         tags=["Tribe"],
         response_class=UJSONResponse,
         summary="tribes of given world")
@limiter.limit('1/minute')
async def get_tribes_by_world(_: Request, world):
    query = db.create_query('tribe', 'SELECT * FROM {}', world)
    response = await db.fetch(query, key='id')
    return parse_result(response, 'name', 'tag', iterable=True)


@app.get('/tribe/{world}/by-id/{tribe_id}',
         tags=["Tribe"],
         response_model=utils.Tribe,
         summary="tribe of given world and tribe id")
@limiter.limit('30/minute')
async def get_tribe_by_id(_: Request, world, tribe_id: int):
    query = db.create_query('tribe', 'SELECT * FROM {} WHERE id = $1', world)
    response = await db.fetchone(query, tribe_id)
    return parse_result(response, 'name', 'tag')


@app.get('/tribe/{world}/by-name/{tribe_name}',
         tags=["Tribe"],
         response_model=utils.Tribe,
         summary="tribe of given world and tribe name")
@limiter.limit('30/minute')
async def get_tribe_by_name(_: Request, world, tribe_name):
    query = db.create_query('tribe', 'SELECT * FROM {} WHERE LOWER(name) = $1', world)
    response = await db.fetchone(query, tribe_name.lower())
    return parse_result(response, 'name', 'tag')


@app.get('/tribe/{world}/by-tag/{tribe_tag}',
         tags=["Tribe"],
         response_model=utils.Tribe,
         summary="tribe of given world and tribe tag")
@limiter.limit('30/minute')
async def get_tribe_by_tag(_: Request, world, tribe_tag):
    query = db.create_query('tribe', 'SELECT * FROM {} WHERE LOWER(tag) = $1', world)
    response = await db.fetchone(query, tribe_tag.lower())
    return parse_result(response, 'name', 'tag')


# TOP
@app.get('/tribe/{world}/top/{attribute}',
         tags=["Tribe"],
         response_model=List[utils.Tribe],
         summary="top tribes of given world and attribute")
@limiter.limit('30/minute')
async def get_top_tribe_by_property(_: Request, world, attribute, amount: int = 5, order: str = "DESC"):
    attribute = utils.verify_arguments(tribe_attribute=attribute, amount=amount, order=order)
    base_query = 'SELECT * FROM {} ORDER BY {} {} LIMIT $1'
    query = db.create_query('tribe', base_query, world, attribute, order)
    response = await db.fetch(query, amount)
    return parse_result(response, 'name', 'tag', iterable=True)


@app.get('/player/{world}/top/{attribute}',
         tags=["Player"],
         response_model=List[utils.Player],
         summary="top players of given world and attribute")
@limiter.limit('30/minute')
async def get_top_player_by_property(_: Request, world, attribute, amount: int = 5, order: str = "DESC"):
    attribute = utils.verify_arguments(player_attribute=attribute, amount=amount, order=order)
    base_query = 'SELECT * FROM {} ORDER BY {} {} LIMIT $1'
    query = db.create_query('player', base_query, world, attribute, order)
    response = await db.fetch(query, amount)
    return parse_result(response, 'name', iterable=True)


# RANDOM
@app.get('/{ds_type}/{world}/random',
         tags=["Misc"],
         response_model=List[Union[utils.Player, utils.Tribe]],
         summary="random tw element of given world")
@limiter.limit('30/minute')
async def get_random_elements_by_world(_: Request, ds_type, world, amount: int = 1):
    utils.verify_arguments(ds_type=ds_type, amount=amount)
    query = db.create_query(ds_type, 'SELECT * FROM {} ORDER BY random() LIMIT $1', world)

    response = await db.fetch(query, amount)
    data = response[0] if amount == 1 else response
    return parse_result(data, 'name', iterable=amount > 1)


# UTIL
@app.get('/attribute/tribe',
         tags=["Util"],
         response_model=List,
         summary=" tribe attributes usable by other endpoints")
@limiter.limit('1/minute')
async def get_all_tribe_attributes(_: Request):
    return list(utils.Tribe.model_fields.keys())


@app.get('/attribute/player',
         tags=["Util"],
         response_model=List,
         summary="player attributes usable by other endpoints")
@limiter.limit('1/minute')
async def get_all_player_attributes(_: Request):
    return list(utils.Player.model_fields.keys())


# RUN
if __name__ == "__main__":
    uvicorn.run("endpoint:app", host=utils.server_url, port=443, log_level="info", **utils.kwargs)
