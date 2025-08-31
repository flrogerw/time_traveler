from pprint import pprint

import discogs_client


catno = 'SST 007'
d = discogs_client.Client('ExampleApplication/0.1', user_token="IBVfRSHXTDQdycqGABYzJoqmodkmVSQXQBRMksYB")

result = d.search(catno=catno, format='Vinyl', country='US', type='release')


for r in result:
    release = d.release(r.id)

    release_dict = {
        "id": release.id,
        "title": release.title,
        "year": release.year,
        "country": release.country,
        "genres": release.genres,
        "styles": release.styles,
        "labels": [label.name for label in release.labels],
        "tracklist": [
            {
                "position": t.position,
                "title": t.title,
                "duration": t.duration
            } for t in release.tracklist
        ],
        "artists": [artist.name for artist in release.artists],
        "catno": release.labels[0].catno if release.labels else None,
        "media_type": release.formats[0]['name'],
        "url": release.url
    }
    if release.labels[0].catno == catno:
        pprint(release_dict)