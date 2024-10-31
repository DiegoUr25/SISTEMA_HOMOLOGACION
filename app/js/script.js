document.getElementById('toggleSidebar').addEventListener('click', function() {
    var sidebar = document.getElementById('sidebar');
    sidebar.classList.toggle('collapsed');
    var mainContent = document.querySelector('.main-content');
    if(sidebar.classList.contains('collapsed')) {
        mainContent.style.marginLeft = '60px';
    } else {
        mainContent.style.marginLeft = '250px';
    }
});

function toggleProfileMenu() {
    const menu = document.getElementById("profileMenu");
    menu.style.display = menu.style.display === "block" ? "none" : "block";
}


window.onclick = function(event) {
    if (!event.target.matches('.user-profile')) {
        const menu = document.getElementById("profileMenu");
        if (menu && menu.style.display === "block") {
            menu.style.display = "none";
        }
    }
}