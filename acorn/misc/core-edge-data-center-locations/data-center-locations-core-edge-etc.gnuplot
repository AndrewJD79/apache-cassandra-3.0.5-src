# Tested with gnuplot 4.6 patchlevel 6

FN_IN = "../worldmap/world_110m.txt"
FN_OUT = system("echo $FN_OUT")

set terminal pdfcairo enhanced size 3in,2in
set output FN_OUT

#set grid lc rgb "#C0C0C0"
set noborder
set notics
#set xtics nomirror scale 0,0 font ",10" format "%.0f{/Symbol \260}" tc rgb "#808080" autofreq -90,30,-30
#set ytics nomirror scale 0,0 font ",10" format "%.0f{/Symbol \260}" tc rgb "#808080" autofreq -90,15

set lmargin 0
set rmargin 0
set tmargin 0
set bmargin 0

# 70
set xrange[-130:-60]

# 35
set yrange[23.5:53.5]

#set key above

plot \
FN_IN with filledcurves fs transparent solid 0.18 noborder lc rgb "#606060" not, \
"~/work/castnet-data/cellular-towers-t-mobile" u 3:2:(0.3) with circles fs transparent solid 0.5 noborder lc rgb "#385E0F" t "T-Mobile cellular towers", \
"data-aws-edge-locations"                      u 2:1:(0.7) with circles fs transparent solid 0.5 noborder lc rgb "#0000FF" t "AWS edge locations", \
"data-aws-edge-locations-1"                    u 2:1:(0.7) with circles fs transparent solid 0.5 noborder lc rgb "#0000FF" not, \
"data-aws-regions"                             u 1:2:(2)   with circles fs transparent solid 0.5 noborder lc rgb "#FF0000" t "AWS regions"
