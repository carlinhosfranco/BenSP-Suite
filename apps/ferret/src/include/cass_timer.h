#ifndef _TIMER_H_
#define _TIMER_H_

#include <sys/time.h> 

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
	struct	timeval	start, end; 
	struct	timeval	lat_start, lat_end;
	float	diff;
	//BenSP
	double what_time;
} stimer_t; 

void stimer_tick(stimer_t *timer); 

float stimer_tuck(stimer_t *timer, const char *msg);

//BenSP Comment: Funções para coleta de tempo para cálculo de latência

double BSP_time_start(stimer_t *time_now);
double BSP_time_end(stimer_t *time_now);

#ifdef __cplusplus
};
#endif

#endif /* _TIMER_H_ */
