from cosiflow.paths import build_path, file_path, first_match, Domain

# Costruisci un path canonico per un file di output
p = file_path(Domain.trigger, year=2027, month=7, entity_id="trg_001",
              leaf="plots", filename="tsmap_2deg.png")
print(p)
# -> cosi/data/trigger/2027_07/trg_001/plots/tsmap_2deg.png

# Trova il primo file che combacia
found = first_match(Domain.obs, year=2027, month=8,
                    entity_id="obs_123", leaf="compton", pattern="*.fits")

# Parsing inverso
info = parse_path(p)
# -> PathInfo(domain='trigger', year=2027, month=7, entity_id='trg_001', leaf='plots', remainder=('tsmap_2deg.png',))
