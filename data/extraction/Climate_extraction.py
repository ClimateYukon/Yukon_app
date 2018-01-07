def get_mon_year( x ):
	month, year = os.path.splitext( os.path.basename( x ) )[0].split( '_' )[-2:]
	return {'month':month, 'year':year, 'fn':x}

def core( path , feature ):
	print('Computing {}'.format(path))
	import glob
	from rasterstats import point_query
	from pathos import multiprocessing as mp

	l = glob.glob( os.path.join( path, '*.tif' ) )
	monyear = list(map( get_mon_year, l ))
	df = pd.DataFrame( monyear )
	df = df.sort_values(by=['year', 'month'], ascending=[1, 1])
	l = df.fn.tolist()
	pool = mp.Pool( 16 )
	out = pool.map( lambda x: point_query( feature.geometry, x ) , l )
	pool.close()
	pool.join()

	df['date'] = df['year'].map(str) + '-' + df['month']
	df = df.set_index(pd.to_datetime(df['date']))
	df = df.drop(['year' , 'month' , 'fn', 'date'],1)
	name = path.split('/')[-3]
	df[name] = [ i for i in out ]
	return df

def build_fn(base_path , feature ,variable , model=None , scenario=None ) :

	if model == None and scenario == None :
		path = os.path.join(base_path, variable)
		return core( path, feature)

	else :
		ls = [core(os.path.join(base_path, model, scenario, variable), feature) for model in models]
		return pd.concat( ls , axis=1 )

if __name__ == '__main__':
	import fiona, os , pickle
	import pandas as pd
	import geopandas as gpd

	shp = '/home/UA/jschroder/Yukon_app/data/extraction.shp'
	variables = ('pr','tas')
	models = ['GISS-E2-R', 'GFDL-CM3' , 'IPSL-CM5A-LR' , 'MRI-CGCM3' , 'NCAR-CCSM4' ]
	scenarios = [ 'rcp45' , 'rcp60' , 'rcp85']
	Base_paths = ['/workspace/Shared/Tech_Projects/DeltaDownscaling/project_data/downscaled']

	feature = gpd.read_file(shp)

	result = {
		variable : {
			scenario : {
				Base_path.split("/")[-1] : build_fn(Base_path , feature , variable = variable , model = models , scenario = scenario)
				for Base_path in Base_paths}
			for scenario in scenarios}
		for variable in variables
		}


	pickle.dump( result, open( "/workspace/Shared/Users/jschroder/TMP/Climate_app2.p", "wb" ) )
