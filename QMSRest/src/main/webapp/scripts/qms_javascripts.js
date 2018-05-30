
    $('a').tooltip();
    $(document).ready(function() { // Start up jQuery
        var classHighlight = 'highlight';
        var $thumbs = $('.measure_click').click(function(e) {
            e.preventDefault();
            $thumbs.removeClass(classHighlight);
            $(this).addClass(classHighlight);
        });
    });

    // Get the modal
    var modal = document.getElementById('myModal');

    // Get the button that opens the modal
    var btn = document.getElementById("myBtn");

    // Get the <span> element that closes the modal
    var span = document.getElementsByClassName("closepop")[0];

    // When the user clicks the button, open the modal 
    btn.onclick = function() {
        modal.style.display = "block";
    }

    // When the user clicks on <span> (x), close the modal
    span.onclick = function() {
        modal.style.display = "none";
    }

    // When the user clicks anywhere outside of the modal, close it
    window.onclick = function(event) {
        if (event.target == modal) {
            modal.style.display = "none";
        }
    }

    function Measure_Creator() {
        document.getElementById("imgcontent").innerHTML = '<object type="text/html" style="width:100%; height:750px;" data="measure_creator.html" ></object>';
        document.getElementById("heading").innerHTML = "Measure Creator";
    }

    function Measure_Editor() {
        document.getElementById("imgcontent").innerHTML = '<object type="text/html" style="width:100%; height:750px;" data="measure_editor.html" ></object>';
        document.getElementById("heading").innerHTML = "Measure Editor";
    }
    function Measure_Library() {
        document.getElementById("sub-content").innerHTML = '<object type="text/html" style="width:100%; height:100%;padding:10px;position: relative;" data="measure_lib.html" ></object>';
    
        document.getElementById("heading").innerHTML = "Measure Library";
        $('.btn-view,.btn-edit').show();
       
        /*var div = $('<div />', {'data-role' : 'fieldcontain'}),
        btn1 = $('<input />', {
              type  : 'button',
              value : 'Copy',
              class : 'btn-view btn btn-primary btn-mini',
              on    : {
                 click: function() {
                     
                 }
              }
          });
        
        var div = $('<div />', {'data-role' : 'fieldcontain'}),
        btn2 = $('<input />', {
              type  : 'button',
              value : 'Edit',
              class : 'btn-edit btn btn-primary btn-mini',
              on    : {
                 click: function() {
                     
                 }
              }
          });
        
        var div = $('<div />', {'data-role' : 'fieldcontain'}),
        btn3 = $('<input />', {
              type  : 'button',
              value : 'View',
              class : 'btn-view btn btn-primary btn-mini',
              on    : {
                 click: function() {
                     
                 }
              }
          });
        div.append(btn1,btn2,btn3).appendTo( $('.button-div').empty());*/
    }


    function Measure_List() {
        document.getElementById("sub-content").innerHTML = '<object type="text/html" style="width:100%; height:100%;padding:10px;position: relative;" data="Measure_Worklist.html" ></object>';
        document.getElementById("heading").innerHTML = "Review Measure List";
        $('.btn-view,.btn-edit').hide();
    }

    function MIPS_html() {
        document.getElementById("main").innerHTML = '<object type="text/html" style="width:100%; height:100%;" data="MIPS.html" ></object>';
        $('#pp').show();
        $(".htag > h3").remove();
        $(".footer").remove();
    }

    function QMS_html() {
        document.getElementById("main").innerHTML = '<object type="text/html" style="width:100%; height:100%;" data="QMS.html" ></object>';
        $('#pp').show();
        $(".htag > h3").remove();
        $(".footer").remove();
    }

    var texts = [];

$(function() {
    $('#list a').each(function(){
        texts.push($(this).text());
    });

   /* alert(texts);*/
    var len= texts.length;
    for (var i = texts.length - 1; i >= 0; i--) {
        if(texts[i]=="MIPS")
        {
           /* var img = $('<img />', {src    : 'Dashboard_icon.png',
                                'class': 'icon-class'
                  });

            $('#mips-list').append(img).show();*/
           $('.icon-class').show();

        }
    }
});
