# March 5, 2026
Considering tradeoffs and possibilities / when to reach for bun:ffi c compiled queries

In theory, we can map out a AOT engine that takes a execution plan, and produce C source code with the predicates that in return gives us a function which takes input[] and returns the result[] as the normal execution/egress engine does now - but where the actual filtering, sorting, etc is done in  C - this would be benefecial for large datasets alone i think, as compiling a c program on-the-fly in bun costs around 10-17ms depending on chain length/complexity / how well we can optimize it.

though in some areas it would most def give us a ~3x perf boost - and allows us much greater control over the memory which also gives use more power for micro optimizations.

It is however a double-edged sword as if we handle memory wrong, we explode - and if we are too eager to reach for the AOT path it will actually hurt performance more than we gain.

for not to think of the actual complexity of doing this right as user-input -> code -> exec is not really "the greatest idea of all time" - especially if it somehow can get exploited to ACE/RCE on server surfaces if some weird part in schemas/filter controls are exposed to users in a way we dont expect.

# sorting n stuff (added)
We should consider adding methods for sorting, ordering, limiting etc in the engine

this makes sense bc a logical next step after filtering would be stuff like that
could possibly also add pagination 

for this to work it would need to be integrated in such a way that these ops happen last in the chain
like:
- predicates
- sort / order 
- limit

# extact predicate builder  (kinda done, rest needs rework to comp)
would be pog bc then we can make reusable chains & precompile then

for this to work effeciently we would need to find a way to internally merge predicate chains so that if 

Predicate chain 1:
...predicates 
filter value between 10, 60
...predicates 

Predicate chain 2:
...predicates 
filter value between 5, 80
...predicates

Internally when we merge it to
filter value between 5, 60 

I think? - wouldn’t that make the most sense as that is the most narrow filter? bweh

# improve predicate chain & paths precomp for long lived server instanxes & worker threads 
we should prob find a way to tag compiled paths etc for instances where the process will run for a long tkme to avoid recompiling parhs unless needed