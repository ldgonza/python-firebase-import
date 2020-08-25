# Return true if the number matches "number = iteration * totalSlots + mySlot", for some iteration number
def is_mine(number, my_slot, total_slots):
  dist = number - my_slot
  mod = dist % total_slots
  return mod == 0

# Get the number from a file named /x/y/z/t/<number>.xxx
def get_number(file):
  name = file.name.split("/").pop()
  number = name.split(".")[0]
  return int(number)

def process(iterator, proc, my_slot, total_slots):
  for file in iterator:
    if is_mine(get_number(file), my_slot, total_slots):
      proc(file)

# ###########################################################
""" 
class TestFile:
  def __init__(self, i):
    self.name = str(i) + ".json"
  def __str__(self):
    return self.name

def print_it(file):
  print(str(file))

files = [TestFile(i) for i in range(10)]
process(files, print_it, 2, 3) """