from shcodes import get_shcodes
import initial

for shcode in get_shcodes():
    initial.run_initial_crawl(shcode, 1, False)

print("Batch done")
