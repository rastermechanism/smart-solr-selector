# Smart solr instance selector 

## Use Case ## 

If you have solr cloud, and needs to distribute the load which can handle 
a. Active node : If few nodes are down, it selects from the remaining
b. Manual loading scheme : If you have preference based on manual selection, like if you 
are sharing some machine with other service or one node is on better hardware, you might want to give 
weight accordingly.