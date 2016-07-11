# Tested with gnuplot 4.6 patchlevel 6

FN_OUT = system("echo $FN_OUT")
MAX_NUM_REQS = system("echo $MAX_NUM_REQS")
LABEL_Y = system("echo $LABEL_Y")
Y_COL = system("echo $Y_COL")
# Cast to a number
Y_COL = Y_COL + 0

Y_ALPHA = system("echo $Y_ALPHA")
Y_ALPHA = Y_ALPHA + 0.0

set terminal pdfcairo enhanced size 3in, 2in
set output FN_OUT

set xlabel "Request density (%)"
set ylabel LABEL_Y offset 0.5,0

set border back
set grid back
set xtics scale 0.5,0 autofreq 0,10
set ytics scale 0.5,0
set tics back

set xrange [0:100]

set key top left

set pointsize 0.4

plot \
"data-cass"  u ($1*100.0/MAX_NUM_REQS):(column(Y_COL)*Y_ALPHA)                  w lp pt 6 lt 0 lc rgb "blue" not , \
"data-cass"  u ($1*100.0/MAX_NUM_REQS):($16 == 0 ? column(Y_COL)*Y_ALPHA : 1/0) w lp pt 7      lc rgb "blue" t "Cassandra", \
"data-acorn" u ($1*100.0/MAX_NUM_REQS):(column(Y_COL)*Y_ALPHA)                  w lp pt 7 lt 0 lc rgb "red"  not, \
"data-acorn" u ($1*100.0/MAX_NUM_REQS):($16 == 0 ? column(Y_COL)*Y_ALPHA : 1/0) w lp pt 7      lc rgb "red"  t "Acorn (U+T)"
