# sorting n stuff
We should consider adding methods for sorting, ordering, limiting etc in the engine

this makes sense bc a logical next step after filtering would be stuff like that
could possibly also add pagination 

for this to work it would need to be integrated in such a way that these ops happen last in the chain
like:
- predicates
- sort / order 
- limit

# extact predicate builder 
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