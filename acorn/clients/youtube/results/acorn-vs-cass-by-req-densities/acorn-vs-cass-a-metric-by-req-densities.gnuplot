# Tested with gnuplot 4.6 patchlevel 6

FN_OUT = system("echo $FN_OUT")
#MAX_NUM_REQS = system("echo $MAX_NUM_REQS")
LABEL_Y = system("echo $LABEL_Y")
Y_COL = system("echo $Y_COL")
# Cast to a number
Y_COL = Y_COL + 0

Y_ALPHA = system("echo $Y_ALPHA")
Y_ALPHA = Y_ALPHA + 0.0

METRIC_Y = system("echo $METRIC_Y")

Y_MAX = system("echo $Y_MAX")
#set print "-"
#print sprintf("[%s]", Y_MAX)

set terminal pdfcairo enhanced size 3in, 2in
set output FN_OUT

#set xlabel "K requests / day"
set xlabel "K requests / sec"
set ylabel LABEL_Y offset 0.5,0

set border back
set grid back
set xtics scale 0.5,0 format "%.1f" #autofreq 0,10
set ytics scale 0.5,0
set tics back

if (strlen(Y_MAX) > 0) {
	set yrange [1:Y_MAX]
} else {
	set yrange [1:]
}

set xrange [0:2.75]

set key top left

set pointsize 0.4

# Instance store cost from Mutants. / GB / Month
INST_STORE_COST=0.527583

# column 3 is eth0_tx, 14 is storage used in MB
y_val(a)=(METRIC_Y eq "cost" ? (0.02 * column(3) / 1024 / 1024 / 1024 + INST_STORE_COST * column(14) / 1024 * 6) \
: column(Y_COL)*Y_ALPHA)

# # of reqs per simulated time in day
reqs0(a)=a / (365.25 / 2) / 1000

# # of reqs per simulation time in mins
reqs1(a)=a / (10.0 * 60) / 1000

set logscale y

plot \
"data-cass"  u (reqs1($1)):(y_val(1))                 w lp pt 6 lt 0 lc rgb "blue" not , \
"data-cass"  u (reqs1($1)):($16 == 0 ? y_val(1): 1/0) w lp pt 7      lc rgb "blue" t "Cassandra", \
"data-acorn" u (reqs1($1)):(y_val(1))                 w lp pt 7 lt 0 lc rgb "red"  not, \
"data-acorn" u (reqs1($1)):($16 == 0 ? y_val(1): 1/0) w lp pt 7      lc rgb "red"  t "Acorn (U+T)"
