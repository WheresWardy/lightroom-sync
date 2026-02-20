# lightroom-immich-sync

This project is designed to mirror the Collection structure and contents in an Adobe Lightroom catalog into Immich albums so that these Collections can be recreated in Immich.

To do this, you will need to use the Lightroom-SQL-Tools library referenced below to list the images and videos in the albums (called Collections) in the Lightroom catalog and update Immich albums with the same contents. You'll need to use the /search/metadata endpoint in the Immich API (also known as searchAssets) to search metadata about the original image or video to find its Immich id (UUID) that you can use to add to the Immich Album.

It's likely you'll need to use some kind of cache such as Redis to store these Lightroom catalog to Immich asset associations so that they don't have to be looked up in the future, but it shouldn't be reliant on the existence of that cache and recreated as needed.

## Tooling

- This project predominantly relies on the Lightroom-SQL-Tools Python library at https://github.com/fdenivac/Lightroom-SQL-tools to inspect the Lightroom catalog which is installed using a virtualenv
- The Immich API docs can be found at https://api.immich.app/ for interaction with Immich

## Locations and endpoints

- The Lightroom catalog file can be found in the environment variable "LIGHTROOM_CATALOG"
- The Immich API endpoint can be found in the environment variable "IMMICH_API_URL"
- The Immich API key can be found in the environment variable "IMMICH_API_KEY"
