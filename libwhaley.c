/* -------------------------------------------------------------------------------------------------
   libwhaley.c - sqlite3 loadable extension for whaley option pricing and implied volatility solving
   gcc -g -fPIC -shared -O2 libwhaley.c -o libwhaley.so -lm
   ------------------------------------------------------------------------------------------------- */

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <stdlib.h>
#include <math.h>

#define M_SQRTPI 1.77245385090551602792

double whaleyCall( double F, double X, double v, double T, double r){
	double e_rT  = exp(-r*T);
	double vT    = v*sqrt(T);
	double M     = 2*r/(v*v);
	double K     = 1-e_rT;
	double q1inf = 0.5-sqrt(0.25+M);
	double q2inf = 1-q1inf;
	double q1inv = 0.5-sqrt(0.25+M/K);
	double q2inv = 1-q1inv;
	q1inv        = 1/q1inv;
	q2inv        = 1/q2inv;
	double CCP   = (-q2inf+exp(2*vT*q1inf))/q1inf;
	double LHS, d1, Nd1, Nd2, nd1, RHS, b;
	int i = 0;
	do{
		LHS = CCP-1;
		d1  = log(CCP)/vT+0.5*vT;
		Nd1 = 0.5+0.5*erf( d1    /M_SQRT2);
		Nd2 = 0.5+0.5*erf((d1-vT)/M_SQRT2);
		nd1 = exp(-0.5*d1*d1)/(M_SQRT2*M_SQRTPI);
		RHS = e_rT*(CCP*Nd1-Nd2)+(1-e_rT*Nd1)*CCP*q2inv;
		b   = e_rT*Nd1*(1-q2inv)+(1-e_rT*nd1/vT)*q2inv;
		CCP = (1+RHS-b*CCP)/(1-b);
	}while( fabs(LHS-RHS) > 0.000001 && i++ < 20 );
	d1  = log(CCP)/vT+0.5*vT;
	Nd1 = 0.5+0.5*erf( d1    /M_SQRT2);
	CCP *= X;
	double A2 = CCP*q2inv*(1-e_rT*Nd1);
	d1  = log(F/X)/vT+0.5*vT;
	Nd1 = 0.5+0.5*erf( d1    /M_SQRT2);
	Nd2 = 0.5+0.5*erf((d1-vT)/M_SQRT2);
	return F < CCP ? e_rT*(F*Nd1-X*Nd2)+A2*pow(F/CCP,1/q2inv) : F-X;
}

double whaleyPut( double F, double X, double v, double T, double r){
	double e_rT  = exp(-r*T);
	double vT    = v*sqrt(T);
	double M     = 2*r/(v*v);
	double K     = 1-e_rT;
	double q1inf = 0.5-sqrt(0.25+M);
	double q2inf = 1-q1inf;
	double q1inv = 0.5-sqrt(0.25+M/K);
	double q2inv = 1-q1inv;
	q1inv        = 1/q1inv;
	q2inv        = 1/q2inv;
	double CCP   = (-q1inf+exp(2*vT*q1inf))/q2inf;
	double LHS, d1, Nd1, Nd2, nd1, RHS, b;
	int i = 0;
	do{
		LHS = 1-CCP;
		d1  = log(CCP)/vT+0.5*vT;
		Nd1 = 0.5-0.5*erf( d1    /M_SQRT2);
		Nd2 = 0.5-0.5*erf((d1-vT)/M_SQRT2);
		nd1 = exp(-0.5*d1*d1)/(M_SQRT2*M_SQRTPI);
		RHS = e_rT*(Nd2-CCP*Nd1)-(1-e_rT*Nd1)*CCP*q1inv;
		b   = e_rT*Nd1*(q1inv-1)-(1+e_rT*nd1/vT)*q1inv;
		CCP = (1-RHS+b*CCP)/(1+b);
	}while( fabs(LHS-RHS) > 0.000001 && i++ < 20 );
	d1  = log(CCP)/vT+0.5*vT;
	Nd1 = 0.5-0.5*erf( d1    /M_SQRT2);
	CCP *= X;
	double A1 = -CCP*q1inv*(1-e_rT*Nd1);
	d1  = log(F/X)/vT+0.5*vT;
	Nd1 = 0.5-0.5*erf( d1    /M_SQRT2);
	Nd2 = 0.5-0.5*erf((d1-vT)/M_SQRT2);
	return F > CCP ? e_rT*(X*Nd2-F*Nd1)+A1*pow(F/CCP,1/q1inv) : X-F;
}

static void whaley( sqlite3_context *context, int argc, sqlite3_value **argv){
	int PC, i;
	double F, X, v, T, r;
	double (*whaleyOpt)(double,double,double,double,double);

	if( argc != 6 ){
		// error: wrong number of arguments
		return;
	}
	for( i = 0; i < 6; i++ ){
		if( sqlite3_value_type(argv[i]) == SQLITE_NULL ){
			// error: NULL values are invalid
			return;
		}
	}
	F  = sqlite3_value_double(argv[0]);
	X  = sqlite3_value_double(argv[1]);
	PC = sqlite3_value_int   (argv[2]);
	v  = sqlite3_value_double(argv[3]);
	T  = sqlite3_value_double(argv[4]);
	r  = sqlite3_value_double(argv[5]);
	if( F<= 0 || X <= 0 || v <= 0 || T <= 0 || r <= 0 ){
		// error: argument <=0
		return;
	}

	whaleyOpt = PC ? whaleyCall : whaleyPut;
	sqlite3_result_double(context,(*whaleyOpt)(F,X,v,T,r));
}

static void whaley_IV( sqlite3_context *context, int argc, sqlite3_value **argv){
	int PC, i;
	double F, X, p, T, r, v, vMin = 0.01, vMax = 2.0, err = 1.0;
	double (*whaleyOpt)(double,double,double,double,double);

	if( argc != 6 ){
		// error: wrong number of inputs
		return;
	}
	for( i = 0; i < 6; i++ ){
		if( sqlite3_value_type(argv[i]) == SQLITE_NULL ){
			// error: input is NULL
			return;
		}
	}
	F  = sqlite3_value_double(argv[0]);
	X  = sqlite3_value_double(argv[1]);
	PC = sqlite3_value_int   (argv[2]);
	p  = sqlite3_value_double(argv[3]);
	T  = sqlite3_value_double(argv[4]);
	r  = sqlite3_value_double(argv[5]);
	if( F<= 0 || X <= 0 || p <= 0 || T <= 0 || r <= 0 ){
		// error: input <=0
		return;
	}

	whaleyOpt = PC ? whaleyCall : whaleyPut;
	if( p == fabs(X-F) || p < (*whaleyOpt)(F,X,vMin,T,r)|| p > (*whaleyOpt)(F,X,vMax,T,r) ){
		// error: implied vol outside range [vMin,vMax]
		return;
	}
	i = 0;
	while( fabs(err) > 0.0001 && i++ < 20 ){
		v = (vMin+vMax)/2;
		err = (*whaleyOpt)(F,X,v,T,r)-p;
		if( err < 0 ){
			vMin = v;
		}
		else{
			vMax = v;
		}
	}
	sqlite3_result_double(context,v);
}

int sqlite3_whaley_init( sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi){
	int rc = SQLITE_OK;
	SQLITE_EXTENSION_INIT2(pApi);
	if( rc == SQLITE_OK ) rc = sqlite3_create_function(db, "whaley", 6, SQLITE_UTF8, 0, whaley, 0, 0);
	if( rc == SQLITE_OK ) rc = sqlite3_create_function(db, "whaley_IV", 6, SQLITE_UTF8, 0, whaley_IV, 0, 0);
	return rc;
}
