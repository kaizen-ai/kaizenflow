# Code Layout

- Based on the container diagram, the code layout matches the different containers
  that we have

```ansi
app/  # contains the user interface to use IM



types/  # standardized types (see data model above)
loader/  # adapts all vendor loaders to a common interface
vendors/
   kibot/
      extractor/
      transformer/
      loader/
      types/  # vendor specific types
      config.py  # s3 paths, credentials for vendor
   eoddata/
      extractor/
      transformer/
      loader/
      types/
      config.py
```