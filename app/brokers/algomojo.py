from algomojo.pyapi import *

def algomojo_orders(**algomojo_kwargs):

    order_kwargs = algomojo_kwargs['order_kwargs'] 
    api_kwargs = algomojo_kwargs['api_kwargs']
    ordertype =  algomojo_kwargs['order_type']['order_type'] 
       
    algomojo = api(**api_kwargs)

    try:
        if ordertype == 'Normal':   
            return algomojo.PlaceOrder(**order_kwargs)
        
        elif ordertype == 'Limit':
            return algomojo.PlaceOrder(**order_kwargs)
        
        elif ordertype == 'Split':
            return algomojo.PlaceOrder(**order_kwargs)
             
        elif ordertype == 'Bracket':
            return algomojo.PlaceBOOrder(**order_kwargs)
            
        elif ordertype == 'Cover':
            return algomojo.PlaceCOOrder(**order_kwargs)
        
        elif ordertype == 'FutureOptions':
            return algomojo.PlaceFOOptionsOrder(**order_kwargs)
        
        elif ordertype == 'Smart':
            return algomojo.PlaceSmartOrder(**order_kwargs)
           
        elif ordertype == 'OrderStatus':
            return algomojo.OrderStatus(**order_kwargs)
        
        elif ordertype == 'OrderHistory':
            return algomojo.OrderHistory(**order_kwargs)
        
    except Exception as e:
        return f'Exception Occured. {e}'



